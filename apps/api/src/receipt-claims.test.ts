import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openDb } from "@orgops/db";
import { createApp } from "./app";

const RUNNER_TOKEN = "receipt-test-token";

async function setup() {
  const dataDir = mkdtempSync(join(tmpdir(), "orgops-receipts-"));
  const db = openDb(":memory:");
  const { app } = createApp({
    db,
    dataDir,
    adminUser: "admin",
    adminPass: "admin",
    runnerToken: RUNNER_TOKEN,
  });
  const login = await app.request("http://localhost/api/auth/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: "admin", password: "admin" }),
  });
  const cookie = login.headers.get("set-cookie") ?? "";
  const json = { "content-type": "application/json", cookie };
  await app.request("http://localhost/api/agents", {
    method: "POST",
    headers: json,
    body: JSON.stringify({
      name: "worker",
      modelId: "openai:gpt-4o-mini",
      workspacePath: ".orgops-data/workspaces/worker",
    }),
  });
  const channelRes = await app.request("http://localhost/api/channels", {
    method: "POST",
    headers: json,
    body: JSON.stringify({ name: "inbox" }),
  });
  const channel = (await channelRes.json()) as { id: string };
  await app.request(`http://localhost/api/channels/${channel.id}/subscribe`, {
    method: "POST",
    headers: json,
    body: JSON.stringify({ subscriberType: "AGENT", subscriberId: "worker" }),
  });
  const eventRes = await app.request("http://localhost/api/events", {
    method: "POST",
    headers: json,
    body: JSON.stringify({
      type: "message.created",
      payload: { text: "do the thing" },
      source: "human:admin",
      channelId: channel.id,
    }),
  });
  const event = (await eventRes.json()) as { id: string };

  const runnerHeaders = (bootId: string) => ({
    "content-type": "application/json",
    "x-orgops-runner-token": RUNNER_TOKEN,
    "x-orgops-runner-id": "runner-1",
    "x-orgops-runner-boot": bootId,
  });
  const register = (bootId: string) =>
    app.request("http://localhost/api/runners/register", {
      method: "POST",
      headers: runnerHeaders(bootId),
      body: JSON.stringify({ existingRunnerId: "runner-1", bootId }),
    });
  const fetchPending = async (bootId: string) => {
    const res = await app.request(
      `http://localhost/api/events?agentName=worker&status=PENDING&channelId=${channel.id}&limit=50`,
      { headers: runnerHeaders(bootId) },
    );
    return ((await res.json()) as Array<{ id: string }>).map((row) => row.id);
  };
  const receipt = () =>
    db
      .prepare(
        "SELECT status, claimed_by AS claimedBy, fail_count AS failCount, retry_at AS retryAt FROM event_receipts WHERE event_id = ? AND agent_name = 'worker'",
      )
      .get(event.id) as {
      status: string;
      claimedBy: string | null;
      failCount: number;
      retryAt: number | null;
    };
  const cleanup = () => rmSync(dataDir, { recursive: true, force: true });
  return { app, db, channel, event, humanHeaders: json, runnerHeaders, register, fetchPending, receipt, cleanup };
}

describe("event receipt claims", () => {
  it("does not mark an event processed when it is only fetched", async () => {
    const t = await setup();
    await t.register("boot-a");
    expect(await t.fetchPending("boot-a")).toEqual([t.event.id]);
    expect(t.receipt()).toMatchObject({ status: "DELIVERED", claimedBy: "runner-1:boot-a" });
    // A second poll by the same live boot does not see it again (no duplicate turn).
    expect(await t.fetchPending("boot-a")).toEqual([]);
    t.cleanup();
  });

  it("returns a crashed boot's claims to PENDING when the runner re-registers", async () => {
    const t = await setup();
    await t.register("boot-a");
    await t.fetchPending("boot-a");
    // Runner crashes after fetching, before completing. New process registers.
    const res = await t.register("boot-b");
    expect(res.status).toBe(201);
    expect(t.receipt()).toMatchObject({ status: "PENDING", claimedBy: null });
    expect(await t.fetchPending("boot-b")).toEqual([t.event.id]);
    t.cleanup();
  });

  it("completes receipts only for the claiming boot and channel", async () => {
    const t = await setup();
    await t.register("boot-a");
    await t.fetchPending("boot-a");
    const stale = await t.app.request("http://localhost/api/events/receipts/complete", {
      method: "POST",
      headers: t.runnerHeaders("boot-old"),
      body: JSON.stringify({ agentName: "worker", channelId: t.channel.id, eventIds: [] }),
    });
    expect(((await stale.json()) as { completed: number }).completed).toBe(0);
    expect(t.receipt().status).toBe("DELIVERED");
    const done = await t.app.request("http://localhost/api/events/receipts/complete", {
      method: "POST",
      headers: t.runnerHeaders("boot-a"),
      body: JSON.stringify({ agentName: "worker", channelId: t.channel.id, eventIds: [] }),
    });
    expect(((await done.json()) as { completed: number }).completed).toBe(1);
    expect(t.receipt().status).toBe("PROCESSED");
    // A processed receipt is not released by a later restart.
    await t.register("boot-c");
    expect(t.receipt().status).toBe("PROCESSED");
    t.cleanup();
  });

  it("releases expired leases and extends live ones on heartbeat", async () => {
    const t = await setup();
    await t.register("boot-a");
    await t.fetchPending("boot-a");
    const before = t.db
      .prepare("SELECT claim_expires_at AS exp FROM event_receipts WHERE event_id = ?")
      .get(t.event.id) as { exp: number };
    await new Promise((resolve) => setTimeout(resolve, 5));
    await t.app.request("http://localhost/api/runners/runner-1/heartbeat", {
      method: "POST",
      headers: t.runnerHeaders("boot-a"),
    });
    const after = t.db
      .prepare("SELECT claim_expires_at AS exp FROM event_receipts WHERE event_id = ?")
      .get(t.event.id) as { exp: number };
    expect(after.exp).toBeGreaterThan(before.exp);
    // Simulate a hung runner whose lease lapsed.
    t.db.prepare("UPDATE event_receipts SET claim_expires_at = 1").run();
    expect(await t.fetchPending("boot-z")).toEqual([t.event.id]);
    expect(t.receipt().claimedBy).toBe("runner-1:boot-z");
    t.cleanup();
  });

  it("retries a failed receipt with backoff and dead-letters after the cap", async () => {
    const t = await setup();
    await t.register("boot-a");
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      t.db.prepare("UPDATE event_receipts SET retry_at = NULL").run();
      expect(await t.fetchPending("boot-a")).toEqual([t.event.id]);
      const res = await t.app.request(`http://localhost/api/events/${t.event.id}/fail`, {
        method: "POST",
        headers: t.runnerHeaders("boot-a"),
        body: JSON.stringify({ agentName: "worker", error: `boom ${attempt}` }),
      });
      const body = (await res.json()) as { receiptStatus: string };
      expect(body.receiptStatus).toBe(attempt < 3 ? "PENDING" : "DEAD");
      if (attempt < 3) {
        expect(t.receipt().retryAt).toBeGreaterThan(Date.now());
        // Backoff hides it from the next poll.
        expect(await t.fetchPending("boot-a")).toEqual([]);
      }
    }
    expect(t.receipt()).toMatchObject({ status: "DEAD", failCount: 3 });
    t.cleanup();
  });

  it("moves a receipt on /fail only for a runner", async () => {
    const t = await setup();
    await t.register("boot-a");
    await t.fetchPending("boot-a");
    const res = await t.app.request(`http://localhost/api/events/${t.event.id}/fail`, {
      method: "POST",
      headers: t.humanHeaders,
      body: JSON.stringify({ agentName: "worker", error: "not a runner" }),
    });
    expect(res.status).toBe(200);
    expect((await res.json()) as object).not.toHaveProperty("receiptStatus");
    expect(t.receipt()).toMatchObject({ status: "DELIVERED", failCount: 0 });
    t.cleanup();
  });
});
