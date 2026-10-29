-- Receipts move PENDING -> DELIVERED (claimed, leased) -> PROCESSED.
-- Receipts delivered before this migration were handled by the old
-- fire-and-forget path; treat them as processed so nothing replays.
ALTER TABLE event_receipts ADD COLUMN claimed_by TEXT NULL;
ALTER TABLE event_receipts ADD COLUMN claim_expires_at INTEGER NULL;
ALTER TABLE event_receipts ADD COLUMN processed_at INTEGER NULL;
ALTER TABLE event_receipts ADD COLUMN fail_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE event_receipts ADD COLUMN last_error TEXT NULL;
ALTER TABLE event_receipts ADD COLUMN retry_at INTEGER NULL;

UPDATE event_receipts
SET status = 'PROCESSED', processed_at = COALESCE(delivered_at, 0)
WHERE status = 'DELIVERED';

CREATE INDEX IF NOT EXISTS idx_event_receipts_claims
  ON event_receipts (status, claimed_by, claim_expires_at);

ALTER TABLE runner_nodes ADD COLUMN boot_id TEXT NULL;
