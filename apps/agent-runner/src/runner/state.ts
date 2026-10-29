import { randomUUID } from "node:crypto";
import type { IntentWatchRecord } from "../intent-watchdog";

export function createRunnerState() {
  const heartbeats = new Map<string, number>();
  const bootstrappedAgents = new Set<string>();
  const bootstrappedAgentKeys = new Map<string, string>();
  const lifecycleChannels = new Map<string, string>();
  const recentTurnWindows = new Map<
    string,
    { startedAt: number; completedAt: number }
  >();
  const intentWatch = new Map<string, IntentWatchRecord>();
  let registeredRunnerId: string | null = null;
  let lastRunnerHeartbeatAt = 0;
  // Identifies this process. Receipts claimed by an earlier boot of the same
  // runner are released when this boot registers.
  const bootId = randomUUID();

  return {
    bootId,
    heartbeats,
    bootstrappedAgents,
    bootstrappedAgentKeys,
    lifecycleChannels,
    recentTurnWindows,
    intentWatch,
    get registeredRunnerId() {
      return registeredRunnerId;
    },
    set registeredRunnerId(value: string | null) {
      registeredRunnerId = value;
    },
    get lastRunnerHeartbeatAt() {
      return lastRunnerHeartbeatAt;
    },
    set lastRunnerHeartbeatAt(value: number) {
      lastRunnerHeartbeatAt = value;
    },
  };
}

export type RunnerState = ReturnType<typeof createRunnerState>;
