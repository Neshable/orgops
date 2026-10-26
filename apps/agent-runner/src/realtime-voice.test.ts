import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createRealtimeVoiceManager, type RealtimeSocket } from "./realtime-voice";
import type { Agent, Event } from "./types";

class FakeSocket implements RealtimeSocket {
  sent: any[] = [];
  closed = false;
  private listeners = new Map<string, Array<(event: any) => void>>();
  constructor(
    readonly url: string,
    readonly headers: Record<string, string>,
  ) {}
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.fire("close", {});
  }
  addEventListener(type: string, listener: (event: any) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }
  fire(type: string, event: unknown) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
  server(message: Record<string, unknown>) {
    this.fire("message", { data: JSON.stringify(message) });
  }
}

const agent: Agent = {
  name: "voice-agent",
  modelId: "openai:gpt-realtime",
  workspacePath: mkdtempSync(join(tmpdir(), "orgops-realtime-voice-")),
  soulContents: "You run the payments on-call desk.",
  desiredState: "RUNNING",
  runtimeState: "RUNNING",
};

function callStarted(overrides: Partial<Event> = {}): Event {
  return {
    id: "evt-call",
    type: "voice.realtime.call.started",
    source: "human:alice",
    channelId: "chan-1",
    payload: {
      targetAgentName: "voice-agent",
      callId: "rtc_1",
      sessionId: "session-1",
      provider: "openai",
      model: "gpt-realtime",
      speakerId: "alice",
      startedAt: 1,
    },
    ...overrides,
  };
}

async function flush() {
  for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setTimeout(resolve, 0));
}

function setup() {
  const sockets: FakeSocket[] = [];
  const emitted: any[] = [];
  const fetches: Array<{ url: string; init?: RequestInit }> = [];
  const toolCalls: Array<{ tool: string; args: Record<string, unknown> }> = [];
  const manager = createRealtimeVoiceManager({
    api: {
      apiFetch: async (path: string) =>
        path.startsWith("/api/files/")
          ? new Response(new Uint8Array([1, 2, 3]))
          : new Response(
          JSON.stringify([
            { id: "m1", type: "message.created", source: "human:bob", channelId: "chan-1", payload: { text: "Deploy is at 3pm." } },
          ]),
        ),
      emitEvent: async (event) => {
        emitted.push(event);
      },
      getPackageSecretsEnv: async () => ({ OPENAI_API_KEY: "sk-agent" }),
      getChannelRecord: async () => ({ id: "chan-1", name: "payments" }) as any,
    },
    skillRoots: [],
    connect: (url, headers) => {
      const socket = new FakeSocket(url, headers);
      sockets.push(socket);
      return socket;
    },
    fetch: (async (url: RequestInfo | URL, init?: RequestInit) => {
      fetches.push({ url: String(url), init });
      return new Response(null, { status: 200 });
    }) as typeof fetch,
    executeTool: async (_ctx, tool, args) => {
      toolCalls.push({ tool, args });
      return { ok: true, files: ["a.txt"] };
    },
  });
  return { manager, sockets, emitted, fetches, toolCalls };
}

async function attachCall(ctx: ReturnType<typeof setup>) {
  expect(ctx.manager.consume(agent, [callStarted()])).toEqual([]);
  await flush();
  const socket = ctx.sockets[0]!;
  socket.fire("open", {});
  await ctx.manager.idle();
  await flush();
  return socket;
}

describe("realtime voice manager", () => {
  it("attaches to the call and syncs instructions, history, and runner tools", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);

    expect(socket.url).toBe("wss://api.openai.com/v1/realtime?call_id=rtc_1");
    expect(socket.headers.Authorization).toBe("Bearer sk-agent");
    const update = socket.sent.find((message) => message.type === "session.update");
    expect(update.session.type).toBe("realtime");
    expect(update.session.instructions).toContain("live voice call with alice");
    expect(update.session.instructions).toContain("payments on-call desk");
    expect(update.session.instructions).toContain("human:bob: Deploy is at 3pm.");
    const shellRun = update.session.tools.find((tool: any) => tool.name === "shell_run");
    expect(shellRun).toMatchObject({ type: "function", parameters: { type: "object" } });
    expect(shellRun.parameters.required).toContain("cmd");
    expect(ctx.manager.hasActiveCall("voice-agent", "chan-1")).toBe(true);
  });

  it("posts the human utterance before the agent reply even when transcription lands late", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);

    socket.server({ type: "input_audio_buffer.committed", item_id: "item_user" });
    socket.server({ type: "response.created", response: { id: "resp_1" } });
    socket.server({ type: "response.output_audio_transcript.done", response_id: "resp_1", transcript: "Two incidents are open." });
    socket.server({ type: "response.done", response: { id: "resp_1", status: "completed", output: [] } });
    await ctx.manager.idle();
    expect(ctx.emitted.filter((event) => event.type === "message.created")).toHaveLength(0);

    socket.server({
      type: "conversation.item.input_audio_transcription.completed",
      item_id: "item_user",
      transcript: "What incidents are open?",
    });
    await ctx.manager.idle();

    const messages = ctx.emitted.filter((event) => event.type === "message.created");
    expect(messages.map((event) => [event.source, event.payload.text])).toEqual([
      ["human:alice", "What incidents are open?"],
      ["agent:voice-agent", "Two incidents are open."],
    ]);
    expect(messages[0].payload.voice).toMatchObject({ mode: "realtime", callId: "rtc_1", speakerId: "alice" });
    expect(messages[1].payload.voice).toMatchObject({ mode: "realtime", agentName: "voice-agent" });
  });

  it("runs tool calls on the host and continues the response", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);

    socket.server({
      type: "response.done",
      response: {
        id: "resp_tool",
        status: "completed",
        output: [{ type: "function_call", call_id: "call_1", name: "fs_list", arguments: '{"path":"."}' }],
      },
    });
    await ctx.manager.idle();

    expect(ctx.toolCalls).toEqual([{ tool: "fs_list", args: { path: "." } }]);
    const output = socket.sent.find((message) => message.type === "conversation.item.create");
    expect(output.item).toMatchObject({ type: "function_call_output", call_id: "call_1" });
    expect(JSON.parse(output.item.output)).toEqual({ ok: true, files: ["a.txt"] });
    expect(socket.sent.at(-1)).toEqual({ type: "response.create" });
    expect(ctx.emitted.map((event) => event.type)).toEqual(
      expect.arrayContaining(["tool.started", "tool.executed"]),
    );
  });

  it("feeds channel text into the call instead of starting a text turn", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);
    const typed: Event = {
      id: "m2",
      type: "message.created",
      source: "human:bob",
      channelId: "chan-1",
      payload: { text: "Rollback finished." },
    };
    const otherChannel: Event = { ...typed, id: "m3", channelId: "chan-2" };

    expect(ctx.manager.consume(agent, [typed, otherChannel])).toEqual([otherChannel]);
    await ctx.manager.idle();

    const note = socket.sent.find((message) => message.type === "conversation.item.create");
    expect(note.item.content[0].text).toBe("[channel] human:bob: Rollback finished.");
  });

  it("shows images to the call and saves attachments into the agent workspace", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);
    const posted: Event = {
      id: "m4",
      type: "message.created",
      source: "human:bob",
      channelId: "chan-1",
      payload: {
        text: "Attached 2 files.",
        attachments: [
          { fileId: "img-1", name: "chart.png", mime: "image/png", size: 3 },
          { fileId: "pdf-1", name: "report.pdf", mime: "application/pdf", size: 3 },
        ],
      },
    };

    ctx.manager.consume(agent, [posted]);
    await ctx.manager.idle();

    const note = socket.sent.find((message) => message.type === "conversation.item.create");
    const imagePath = join(agent.workspacePath, ".orgops-attachments", "img-1", "chart.png");
    const pdfPath = join(agent.workspacePath, ".orgops-attachments", "pdf-1", "report.pdf");
    expect(note.item.content[0].text).toContain(`chart.png (image/png): ${imagePath}`);
    expect(note.item.content[0].text).toContain(`report.pdf (application/pdf): ${pdfPath}`);
    expect(note.item.content.slice(1)).toEqual([
      { type: "input_image", image_url: "data:image/png;base64,AQID" },
    ]);
    expect(readFileSync(pdfPath)).toEqual(Buffer.from([1, 2, 3]));
  });

  it("hangs up when the human ends the call and reports runner-side disconnects", async () => {
    const ctx = setup();
    const socket = await attachCall(ctx);

    ctx.manager.consume(agent, [
      {
        id: "evt-end",
        type: "voice.realtime.call.ended",
        source: "human:alice",
        channelId: "chan-1",
        payload: { targetAgentName: "voice-agent", callId: "rtc_1", endedAt: 2, reason: "left" },
      },
    ]);
    await ctx.manager.idle();

    expect(ctx.fetches[0]?.url).toBe("https://api.openai.com/v1/realtime/calls/rtc_1/hangup");
    expect(socket.closed).toBe(true);
    expect(ctx.manager.hasActiveCall("voice-agent", "chan-1")).toBe(false);
    expect(ctx.emitted.some((event) => event.type === "voice.realtime.call.ended")).toBe(false);

    const second = setup();
    const dropped = await attachCall(second);
    dropped.close();
    await second.manager.idle();
    expect(second.emitted.find((event) => event.type === "voice.realtime.call.ended")).toMatchObject({
      source: "agent:voice-agent",
      payload: { callId: "rtc_1", reason: "disconnected" },
    });
  });

  it("ignores calls meant for another agent", async () => {
    const ctx = setup();
    ctx.manager.consume(agent, [
      callStarted({ payload: { ...(callStarted().payload as object), targetAgentName: "someone-else" } }),
    ]);
    await flush();
    expect(ctx.sockets).toHaveLength(0);
  });
});
