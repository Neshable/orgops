import { beforeEach, describe, expect, it, vi } from "vitest";
import { generate } from "@orgops/llm";
import { createTurnExecutor } from "./turn-executor";
import type { Agent, Event } from "./types";

vi.mock("@orgops/llm", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@orgops/llm")>()),
  generate: vi.fn(),
}));

const agent: Agent = {
  name: "tester",
  workspacePath: "/tmp",
  modelId: "openai:gpt-4o-mini",
  desiredState: "RUNNING",
  runtimeState: "RUNNING",
  memoryContextMode: "OFF",
  emitAuditEvents: false,
};

const trigger: Event = {
  id: "trigger",
  type: "message.created",
  source: "human:test",
  channelId: "chat",
  payload: { text: "question" },
};

function setup() {
  const emitted: Array<{ type?: string; payload?: Record<string, unknown> }> = [];
  const execute = createTurnExecutor({
    projectRoot: "/tmp",
    skillRoots: [],
    llmCallTimeoutMs: 10_000,
    api: {
      apiFetch: async (path: string) => {
        if (path.startsWith("/api/events?")) return Response.json([]);
        throw new Error(`Unexpected API call ${path}`);
      },
      emitEvent: async (event) => {
        emitted.push(event as { type?: string; payload?: Record<string, unknown> });
      },
      listChannels: async () => [],
      getChannelRecord: async () => ({
        id: "chat",
        participants: [{ subscriberType: "AGENT", subscriberId: "tester" }],
      }),
      getChannelParticipationValidationError: async () => null,
      ensureLifecycleChannel: async () => "lifecycle",
      getPackageSecretsEnv: async () => ({}),
    },
  });
  return {
    emitted,
    run: () => execute(agent, [trigger]),
  };
}

beforeEach(() => {
  vi.mocked(generate).mockReset();
});

describe("CLASSIC empty final output", () => {
  it("does not post a structured-output fallback after tool-calls with empty text", async () => {
    const ctx = setup();
    vi.mocked(generate).mockResolvedValue({
      text: "",
      toolCalls: [{ toolName: "shell_run" }],
      toolResults: [{ toolName: "shell_run" }],
      finishReason: "tool-calls",
    });
    await ctx.run();
    expect(generate).toHaveBeenCalledTimes(1);
    expect(ctx.emitted.filter((event) => event.type === "message.created")).toHaveLength(0);
    expect(ctx.emitted.find((event) => event.type === "agent.turn.completed")?.payload).toMatchObject({
      noReply: true,
    });
  });

  it("still posts a format fallback when the model returns invalid JSON without tools", async () => {
    const ctx = setup();
    vi.mocked(generate).mockResolvedValue({
      text: "not json",
      toolCalls: [],
      toolResults: [],
      finishReason: "stop",
    });
    await ctx.run();
    expect(generate).toHaveBeenCalledTimes(3);
    const messages = ctx.emitted.filter((event) => event.type === "message.created");
    expect(messages).toHaveLength(1);
    expect(messages[0]?.payload?.text).toBe("not json");
  });
});
