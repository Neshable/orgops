import type { OrgOpsDb } from "@orgops/db";

// Receipt lifecycle: PENDING -> DELIVERED (claimed by a runner boot, leased)
// -> PROCESSED. A failed batch returns receipts to PENDING until the failure
// cap, then DEAD. Delivery is not processing: only an explicit completion
// moves a receipt to PROCESSED.

export const DEFAULT_RECEIPT_CLAIM_LEASE_MS = 10 * 60_000;
export const DEFAULT_RECEIPT_MAX_ATTEMPTS = 3;
const RETRY_BASE_DELAY_MS = 30_000;

export function readReceiptMaxAttempts() {
  const parsed = Number(process.env.ORGOPS_RECEIPT_MAX_ATTEMPTS);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_RECEIPT_MAX_ATTEMPTS;
}

export function readReceiptClaimLeaseMs() {
  const parsed = Number(process.env.ORGOPS_EVENT_CLAIM_LEASE_MS);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_RECEIPT_CLAIM_LEASE_MS;
}

export function claimOwner(runnerId: string | undefined, bootId: string | undefined) {
  const runner = runnerId?.trim();
  const boot = bootId?.trim();
  if (!runner || !boot) return null;
  return `${runner}:${boot}`;
}

export function createReceiptClaims(db: OrgOpsDb, leaseMs = readReceiptClaimLeaseMs()) {
  const releaseExpiredStmt = db.prepare(
    `UPDATE event_receipts
     SET status = 'PENDING', claimed_by = NULL, claim_expires_at = NULL
     WHERE status = 'DELIVERED' AND claim_expires_at IS NOT NULL AND claim_expires_at < ?`,
  );
  const releasePreviousBootsStmt = db.prepare(
    `UPDATE event_receipts
     SET status = 'PENDING', claimed_by = NULL, claim_expires_at = NULL
     WHERE status = 'DELIVERED' AND claimed_by LIKE ? AND claimed_by <> ?`,
  );
  const extendStmt = db.prepare(
    `UPDATE event_receipts SET claim_expires_at = ?
     WHERE status = 'DELIVERED' AND claimed_by = ?`,
  );
  const completeChannelStmt = db.prepare(
    `UPDATE event_receipts SET status = 'PROCESSED', processed_at = ?, claim_expires_at = NULL
     WHERE agent_name = ? AND claimed_by = ? AND status = 'DELIVERED'
       AND event_id IN (SELECT id FROM events WHERE channel_id = ?)`,
  );
  const completeIdsStmt = db.prepare(
    `UPDATE event_receipts SET status = 'PROCESSED', processed_at = ?, claim_expires_at = NULL
     WHERE agent_name = ? AND event_id = ? AND status = 'DELIVERED'`,
  );
  const getReceiptStmt = db.prepare(
    `SELECT status, fail_count AS failCount FROM event_receipts WHERE event_id = ? AND agent_name = ?`,
  );
  const failReceiptStmt = db.prepare(
    `UPDATE event_receipts
     SET status = ?, fail_count = ?, last_error = ?, retry_at = ?,
         claimed_by = NULL, claim_expires_at = NULL
     WHERE event_id = ? AND agent_name = ?`,
  );

  return {
    leaseMs,
    claimExpiry(now: number) {
      return now + leaseMs;
    },
    /** Return leases whose owner stopped extending them (dead or hung runner). */
    releaseExpired(now: number) {
      return releaseExpiredStmt.run(now).changes;
    },
    /** A runner registered with a new boot: its earlier boots' claims are dead. */
    releasePreviousBoots(runnerId: string, bootId: string) {
      return releasePreviousBootsStmt.run(`${runnerId}:%`, `${runnerId}:${bootId}`).changes;
    },
    extend(owner: string, now: number) {
      return extendStmt.run(now + leaseMs, owner).changes;
    },
    completeChannel(input: { agentName: string; owner: string; channelId: string; now: number }) {
      return completeChannelStmt.run(input.now, input.agentName, input.owner, input.channelId).changes;
    },
    completeIds(input: { agentName: string; eventIds: string[]; now: number }) {
      let changes = 0;
      const tx = db.transaction(() => {
        for (const eventId of input.eventIds) {
          changes += completeIdsStmt.run(input.now, input.agentName, eventId).changes;
        }
      });
      tx();
      return changes;
    },
    /** Returns the receipt's new status, or null when no receipt exists. */
    fail(input: {
      eventId: string;
      agentName: string;
      error: string | null;
      maxAttempts: number;
      now: number;
    }) {
      const row = getReceiptStmt.get(input.eventId, input.agentName) as
        | { status: string; failCount: number }
        | undefined;
      if (!row) return null;
      if (row.status === "PROCESSED" || row.status === "DEAD") return row.status;
      const failCount = row.failCount + 1;
      const status = failCount >= input.maxAttempts ? "DEAD" : "PENDING";
      const retryAt =
        status === "PENDING" ? input.now + RETRY_BASE_DELAY_MS * 4 ** (failCount - 1) : null;
      failReceiptStmt.run(status, failCount, input.error, retryAt, input.eventId, input.agentName);
      return status;
    },
  };
}

export type ReceiptClaims = ReturnType<typeof createReceiptClaims>;
