import { join } from "node:path";
import { toolParametersJsonSchema } from "@orgops/llm";
import { listSkillsFromRoots, loadSkillEventShapes } from "@orgops/skills";
import {
  getCoreEventShapes,
  serializeEventShapes,
  validateEventAgainstShapes,
} from "@orgops/schemas";
import { isWorkspaceAttachment, readMessageAttachments, saveAttachmentToWorkspace } from "./attachments";
import { createRunnerTools, executeTool, type ExecuteContext } from "./tools";
import { redactPersistedToolOutput } from "./tools/types";
import type { RunnerApi } from "./runner/api";
import type { Agent, Event } from "./types";

export const OPENAI_REALTIME_BASE_URL = "https://api.openai.com/v1/realtime";
const OPENAI_REALTIME_SOCKET_URL = "wss://api.openai.com/v1/realtime";
const USER_TRANSCRIPT_WAIT_MS = 8_000;
const MAX_TOOL_OUTPUT_CHARS = 12_000;
const MAX_HISTORY_MESSAGES = 30;
const MAX_HISTORY_MESSAGE_CHARS = 600;
const MAX_SOUL_CHARS = 6_000;
const MAX_INLINE_IMAGE_BYTES = 5 * 1024 * 1024;

export type RealtimeSocket = {
  send: (data: string) => void;
  close: (code?: number, reason?: string) => void;
  addEventListener: (type: string, listener: (event: any) => void) => void;
};

type RealtimeVoiceDeps = {
  api: Pick<RunnerApi, "apiFetch" | "emitEvent" | "getPackageSecretsEnv" | "getChannelRecord">;
  skillRoots: Array<{ path: string }>;
  connect?: (url: string, headers: Record<string, string>) => RealtimeSocket;
  fetch?: typeof fetch;
  executeTool?: (ctx: ExecuteContext, tool: string, args: Record<string, unknown>) => Promise<unknown>;
};

type OutboxSlot = {
  key: string;
  resolved: boolean;
  draft: Record<string, unknown> | null;
  timer?: ReturnType<typeof setTimeout>;
};

type RealtimeCall = {
  callId: string;
  agent: Agent;
  channelId: string;
  sessionId: string;
  speakerSource: string;
  speakerId: string;
  model: string;
  apiKey: string;
  socket: RealtimeSocket | null;
  closedByHuman: boolean;
  ended: boolean;
  outbox: OutboxSlot[];
  posting: Promise<void>;
  responseText: Map<string, string[]>;
  handledToolCallIds: Set<string>;
  executeCtx: ExecuteContext | null;
};

type CallStartedPayload = {
  targetAgentName?: string;
  callId?: string;
  sessionId?: string;
  speakerId?: string;
  model?: string;
};

function payloadOf(event: Event): Record<string, unknown> {
  const payload = event.payload;
  return payload && typeof payload === "object" && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)
    : {};
}

function defaultConnect(url: string, headers: Record<string, string>): RealtimeSocket {
  // Node's WebSocket (undici) accepts headers in the init object.
  const WebSocketCtor = globalThis.WebSocket as unknown as new (
    url: string,
    init: { headers: Record<string, string> },
  ) => RealtimeSocket;
  return new WebSocketCtor(url, { headers });
}

function truncate(text: string, max: number) {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

export function isRealtimeVoiceMessage(event: Event): boolean {
  if (event.type !== "message.created") return false;
  const voice = payloadOf(event).voice as { mode?: unknown } | undefined;
  return voice?.mode === "realtime";
}

export function buildRealtimeInstructions(input: {
  agent: Agent;
  speakerId: string;
  channel: { id: string; name?: string; description?: string } | null;
  channelId: string;
  skillIndex: string;
  recentMessages: Event[];
}) {
  const soul = (input.agent.soulContents ?? "").trim().slice(0, MAX_SOUL_CHARS);
  const channelName = input.channel?.name ?? input.channelId;
  const history = input.recentMessages
    .map((event) => {
      const text = String(payloadOf(event).text ?? "").trim();
      if (!text) return "";
      return `- ${event.source}: ${truncate(text.replace(/\s+/g, " "), MAX_HISTORY_MESSAGE_CHARS)}`;
    })
    .filter(Boolean)
    .join("\n");
  return [
    `You are ${input.agent.name}, an OrgOps agent in a live voice call with ${input.speakerId} in channel "${channelName}" (${input.channelId}).`,
    [
      "Voice call rules:",
      "- Speak naturally and keep replies short. No markdown, lists, or code read aloud; summarize instead.",
      "- The human can interrupt you; stop and listen when they do.",
      "- Both sides of this call are transcribed into the channel automatically. Do not use tools to post what you say.",
      "- Use your tools to act on your host and OrgOps. Say briefly what you are about to do before slow work, then report the outcome.",
      "- Channel text posted during the call reaches you as notes starting with [channel]. Attachments are saved into your workspace (paths listed in the note) and images are shown to you; open other files with your tools.",
    ].join("\n"),
    `Your workspace: ${input.agent.workspacePath}`,
    input.channel?.description ? `Channel description: ${input.channel.description}` : "",
    soul ? `Your soul:\n${soul}` : "",
    input.skillIndex ? `Your skills (name | description | SKILL.md path):\n${input.skillIndex}` : "",
    history ? `Recent channel messages (oldest first):\n${history}` : "",
  ]
    .filter(Boolean)
    .join("\n\n");
}

export function createRealtimeVoiceManager(deps: RealtimeVoiceDeps) {
  const connect = deps.connect ?? defaultConnect;
  const fetchImpl = deps.fetch ?? fetch;
  const runTool = deps.executeTool ?? executeTool;
  const calls = new Map<string, RealtimeCall>();
  const inFlight = new Set<Promise<unknown>>();

  function track(promise: Promise<unknown>, label: string) {
    const tracked = promise.catch((error) => {
      console.warn(label, { error: String(error) });
    });
    inFlight.add(tracked);
    void tracked.finally(() => inFlight.delete(tracked));
  }

  function callsFor(agentName: string, channelId?: string) {
    return [...calls.values()].filter(
      (call) =>
        call.agent.name === agentName && (channelId === undefined || call.channelId === channelId),
    );
  }

  function hasActiveCall(agentName: string, channelId: string) {
    return callsFor(agentName, channelId).length > 0;
  }

  function send(call: RealtimeCall, message: Record<string, unknown>) {
    if (!call.socket || call.ended) return;
    try {
      call.socket.send(JSON.stringify(message));
    } catch (error) {
      console.warn("realtime.send_failed", { callId: call.callId, error: String(error) });
    }
  }

  async function emitBestEffort(event: Record<string, unknown>) {
    try {
      await deps.api.emitEvent(event);
    } catch (error) {
      console.warn("realtime.emit_failed", { type: event.type, error: String(error) });
    }
  }

  function flushOutbox(call: RealtimeCall) {
    const posting = call.posting.then(async () => {
      while (call.outbox[0]?.resolved) {
        const slot = call.outbox.shift()!;
        if (slot.timer) clearTimeout(slot.timer);
        if (slot.draft) await emitBestEffort(slot.draft);
      }
    });
    call.posting = posting;
    inFlight.add(posting);
    void posting.finally(() => inFlight.delete(posting));
  }

  function openSlot(call: RealtimeCall, key: string, waitMs?: number) {
    if (call.outbox.some((slot) => slot.key === key)) return;
    const slot: OutboxSlot = { key, resolved: false, draft: null };
    if (waitMs) {
      slot.timer = setTimeout(() => resolveSlot(call, key, null), waitMs);
    }
    call.outbox.push(slot);
  }

  function resolveSlot(call: RealtimeCall, key: string, draft: Record<string, unknown> | null) {
    let slot = call.outbox.find((entry) => entry.key === key);
    if (!slot) {
      if (!draft) return;
      slot = { key, resolved: false, draft: null };
      call.outbox.push(slot);
    }
    if (slot.resolved) return;
    slot.resolved = true;
    slot.draft = draft;
    flushOutbox(call);
  }

  function voiceMeta(call: RealtimeCall, extra: Record<string, unknown> = {}) {
    return {
      mode: "realtime",
      status: "transcribed",
      callId: call.callId,
      sessionId: call.sessionId,
      agentName: call.agent.name,
      transcribedBy: `openai:${call.model}`,
      ...extra,
    };
  }

  async function buildExecuteContext(call: RealtimeCall, triggerEvent: Event, env: Record<string, string>) {
    const agent = call.agent;
    const roots = [
      ...deps.skillRoots.map((root) => root.path),
      ...(agent.additionalSkillRoots ?? []),
    ]
      .map((root) => root.trim())
      .filter((root, index, all) => root && all.indexOf(root) === index)
      .map((path) => ({ path }));
    const enabled = new Set(agent.enabledSkills ?? []);
    const skills = listSkillsFromRoots(roots).filter((skill: any) => enabled.has(skill.name));
    const loadedShapes = await loadSkillEventShapes(skills);
    const eventShapes = [...getCoreEventShapes(), ...loadedShapes.shapes];
    const serialized = serializeEventShapes(eventShapes);
    const executeCtx: ExecuteContext = {
      agent,
      triggerEvent,
      channelId: call.channelId,
      extraAllowedRoots: skills.map((skill: any) => skill.path),
      injectionEnv: env,
      apiFetch: deps.api.apiFetch,
      emitEvent: deps.api.emitEvent,
      emitAudit: (type, payload, source = `agent:${agent.name}`) =>
        agent.emitAuditEvents !== false
          ? deps.api.emitEvent({ type, payload, source, channelId: call.channelId })
          : Promise.resolve(),
      listEventTypes: (query) =>
        serialized.filter((entry) => {
          if (query?.source && entry.source !== query.source) return false;
          if (query?.typePrefix && !entry.type.startsWith(query.typePrefix)) return false;
          return true;
        }),
      validateEvent: (draft) => validateEventAgainstShapes(draft, eventShapes),
    };
    const skillIndex = skills
      .map((skill: any) => `${skill.name} | ${skill.description} | ${join(skill.path, "SKILL.md")}`)
      .join("\n");
    return { executeCtx, skillIndex };
  }

  async function listRecentMessages(channelId: string): Promise<Event[]> {
    try {
      const res = await deps.api.apiFetch(
        `/api/events?channelId=${encodeURIComponent(channelId)}&type=message.created&limit=${MAX_HISTORY_MESSAGES}&order=desc`,
      );
      const rows = (await res.json()) as Event[];
      return Array.isArray(rows) ? rows.reverse() : [];
    } catch {
      return [];
    }
  }

  async function buildSessionUpdate(call: RealtimeCall, triggerEvent: Event, env: Record<string, string>) {
    const [{ executeCtx, skillIndex }, channel, recentMessages] = await Promise.all([
      buildExecuteContext(call, triggerEvent, env),
      deps.api.getChannelRecord(call.channelId).catch(() => null),
      listRecentMessages(call.channelId),
    ]);
    call.executeCtx = executeCtx;
    const toolsByName = createRunnerTools({
      agent: call.agent,
      event: triggerEvent,
      channelId: call.channelId,
      runTool: async () => undefined,
      apiFetch: deps.api.apiFetch,
      emitEvent: deps.api.emitEvent,
    });
    const tools = await Promise.all(
      Object.entries(toolsByName).map(async ([name, tool]) => ({
        type: "function",
        name,
        description: tool.description ?? "",
        parameters: await toolParametersJsonSchema(tool),
      })),
    );
    return {
      type: "session.update",
      session: {
        type: "realtime",
        instructions: buildRealtimeInstructions({
          agent: call.agent,
          speakerId: call.speakerId,
          channel: channel as { id: string; name?: string; description?: string } | null,
          channelId: call.channelId,
          skillIndex,
          recentMessages: recentMessages.filter((event) => !isRealtimeVoiceMessage(event)),
        }),
        tools,
        tool_choice: "auto",
      },
    };
  }

  async function runToolCall(call: RealtimeCall, item: { call_id?: string; name?: string; arguments?: string }) {
    const toolCallId = item.call_id ?? "";
    const name = item.name ?? "";
    if (!toolCallId || !name || call.handledToolCallIds.has(toolCallId)) return false;
    call.handledToolCallIds.add(toolCallId);
    let args: Record<string, unknown> = {};
    try {
      args = item.arguments ? (JSON.parse(item.arguments) as Record<string, unknown>) : {};
    } catch {
      args = {};
    }
    const audit = call.agent.emitAuditEvents !== false;
    if (audit) {
      await emitBestEffort({
        type: "tool.started",
        source: `agent:${call.agent.name}`,
        channelId: call.channelId,
        payload: { tool: name, args, runtime: "realtime", callId: call.callId },
      });
    }
    let output: unknown;
    try {
      output = call.executeCtx
        ? await runTool(call.executeCtx, name, args)
        : { error: "Tool context is not ready yet" };
    } catch (error) {
      output = { error: String(error) };
    }
    if (audit) {
      const persisted = redactPersistedToolOutput(name, output);
      await emitBestEffort({
        type: "tool.executed",
        source: `agent:${call.agent.name}`,
        channelId: call.channelId,
        payload: {
          tool: name,
          args,
          output: persisted.output,
          runtime: "realtime",
          callId: call.callId,
          ...(persisted.redacted ? { sensitiveOutputRedacted: true } : {}),
        },
      });
    }
    let serialized: string;
    try {
      serialized = typeof output === "string" ? output : JSON.stringify(output ?? null);
    } catch {
      serialized = String(output);
    }
    send(call, {
      type: "conversation.item.create",
      item: {
        type: "function_call_output",
        call_id: toolCallId,
        output: truncate(serialized, MAX_TOOL_OUTPUT_CHARS),
      },
    });
    return true;
  }

  async function handleResponseDone(call: RealtimeCall, response: any) {
    const responseId = String(response?.id ?? "");
    const spoken = (call.responseText.get(responseId) ?? []).join(" ").trim();
    call.responseText.delete(responseId);
    const interrupted = response?.status === "cancelled";
    resolveSlot(
      call,
      `assistant:${responseId}`,
      spoken
        ? {
            type: "message.created",
            source: `agent:${call.agent.name}`,
            channelId: call.channelId,
            payload: {
              text: spoken,
              voice: voiceMeta(call, interrupted ? { interrupted: true } : {}),
            },
          }
        : null,
    );
    const toolItems = (Array.isArray(response?.output) ? response.output : []).filter(
      (item: any) => item?.type === "function_call",
    );
    let ranAny = false;
    for (const item of toolItems) {
      if (await runToolCall(call, item)) ranAny = true;
    }
    if (ranAny) send(call, { type: "response.create" });
  }

  function handleServerEvent(call: RealtimeCall, message: any) {
    switch (message?.type) {
      case "input_audio_buffer.committed":
        openSlot(call, `user:${message.item_id}`, USER_TRANSCRIPT_WAIT_MS);
        return;
      case "conversation.item.input_audio_transcription.completed": {
        const text = String(message.transcript ?? "").trim();
        resolveSlot(
          call,
          `user:${message.item_id}`,
          text
            ? {
                type: "message.created",
                source: call.speakerSource,
                channelId: call.channelId,
                payload: {
                  text,
                  voice: voiceMeta(call, { speakerId: call.speakerId }),
                },
              }
            : null,
        );
        return;
      }
      case "conversation.item.input_audio_transcription.failed":
        resolveSlot(call, `user:${message.item_id}`, null);
        return;
      case "response.created":
        if (message.response?.id) openSlot(call, `assistant:${message.response.id}`);
        return;
      case "response.output_audio_transcript.done":
      case "response.audio_transcript.done":
      case "response.output_text.done": {
        const responseId = String(message.response_id ?? "");
        const text = String(message.transcript ?? message.text ?? "").trim();
        if (!responseId || !text) return;
        const parts = call.responseText.get(responseId) ?? [];
        parts.push(text);
        call.responseText.set(responseId, parts);
        return;
      }
      case "response.done":
        track(handleResponseDone(call, message.response), "realtime.response_done_failed");
        return;
      case "error":
        console.warn("realtime.server_error", { callId: call.callId, error: message.error });
        return;
      default:
        return;
    }
  }

  function finalizeCall(call: RealtimeCall, reason: string) {
    if (call.ended) return;
    call.ended = true;
    calls.delete(call.callId);
    for (const slot of call.outbox) {
      if (!slot.resolved) {
        slot.resolved = true;
        if (slot.timer) clearTimeout(slot.timer);
      }
    }
    flushOutbox(call);
    if (!call.closedByHuman) {
      track(emitBestEffort({
        type: "voice.realtime.call.ended",
        source: `agent:${call.agent.name}`,
        channelId: call.channelId,
        payload: {
          targetAgentName: call.agent.name,
          callId: call.callId,
          sessionId: call.sessionId,
          endedAt: Date.now(),
          reason,
          text: `Voice call with ${call.agent.name} ended.`,
        },
      }), "realtime.ended_emit_failed");
    }
  }

  async function attach(agent: Agent, event: Event) {
    const payload = payloadOf(event) as CallStartedPayload;
    const callId = String(payload.callId ?? "").trim();
    const channelId = event.channelId;
    if (!callId || !channelId || calls.has(callId)) return;
    const env = await deps.api.getPackageSecretsEnv(agent.name, channelId).catch(() => ({}) as Record<string, string>);
    const apiKey = env.OPENAI_API_KEY || process.env.OPENAI_API_KEY || "";
    const call: RealtimeCall = {
      callId,
      agent,
      channelId,
      sessionId: String(payload.sessionId ?? callId),
      speakerSource: event.source,
      speakerId: String(payload.speakerId ?? event.source.replace(/^human:/, "")),
      model: String(payload.model ?? "gpt-realtime"),
      apiKey,
      socket: null,
      closedByHuman: false,
      ended: false,
      outbox: [],
      posting: Promise.resolve(),
      responseText: new Map(),
      handledToolCallIds: new Set(),
      executeCtx: null,
    };
    calls.set(callId, call);
    if (!apiKey) {
      console.warn("realtime.missing_openai_key", { agent: agent.name, callId });
      finalizeCall(call, "missing_openai_api_key");
      return;
    }
    const sessionUpdatePromise = buildSessionUpdate(call, event, env);
    let socket: RealtimeSocket;
    try {
      socket = connect(`${OPENAI_REALTIME_SOCKET_URL}?call_id=${encodeURIComponent(callId)}`, {
        Authorization: `Bearer ${apiKey}`,
      });
    } catch (error) {
      console.warn("realtime.connect_failed", { callId, error: String(error) });
      finalizeCall(call, "connect_failed");
      return;
    }
    call.socket = socket;
    socket.addEventListener("open", () => {
      void sessionUpdatePromise
        .then((update) => send(call, update))
        .catch((error) => console.warn("realtime.session_update_failed", { callId, error: String(error) }));
    });
    socket.addEventListener("message", (message: { data?: unknown }) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(message.data ?? ""));
      } catch {
        return;
      }
      handleServerEvent(call, parsed);
    });
    socket.addEventListener("error", (error: { message?: string }) => {
      console.warn("realtime.socket_error", { callId, error: error?.message ?? String(error) });
    });
    socket.addEventListener("close", () => finalizeCall(call, "disconnected"));
  }

  async function hangup(call: RealtimeCall, reason: string, closedByHuman: boolean) {
    call.closedByHuman = call.closedByHuman || closedByHuman;
    if (call.apiKey) {
      await fetchImpl(`${OPENAI_REALTIME_BASE_URL}/calls/${encodeURIComponent(call.callId)}/hangup`, {
        method: "POST",
        headers: { Authorization: `Bearer ${call.apiKey}` },
      }).catch(() => undefined);
    }
    try {
      call.socket?.close(1000, reason);
    } catch {}
    finalizeCall(call, reason);
  }

  async function injectChannelMessage(call: RealtimeCall, event: Event) {
    const text = String(payloadOf(event).text ?? "").trim();
    const attachments = readMessageAttachments(event.payload).filter(isWorkspaceAttachment);
    if (!text && attachments.length === 0) return;
    const images: Array<{ type: "input_image"; image_url: string }> = [];
    const fileLines: string[] = [];
    for (const attachment of attachments) {
      const saved = await saveAttachmentToWorkspace(deps.api.apiFetch, call.agent, attachment).catch(() => null);
      const label = `${attachment.name ?? attachment.fileId}${attachment.mime ? ` (${attachment.mime})` : ""}`;
      if (!saved) {
        fileLines.push(`- ${label}: could not be downloaded`);
        continue;
      }
      fileLines.push(`- ${label}: ${saved.localPath}`);
      const mime = (attachment.mime ?? "").toLowerCase();
      if (mime.startsWith("image/") && saved.bytes.byteLength <= MAX_INLINE_IMAGE_BYTES && images.length < 3) {
        images.push({
          type: "input_image",
          image_url: `data:${mime};base64,${Buffer.from(saved.bytes).toString("base64")}`,
        });
      }
    }
    const lines = [`[channel] ${event.source}: ${truncate(text, 4000)}`];
    if (fileLines.length > 0) {
      lines.push(`Attachments saved in your workspace (images are also attached here):\n${fileLines.join("\n")}`);
    }
    send(call, {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: lines.join("\n") }, ...images],
      },
    });
  }

  /**
   * Takes the events this manager owns out of an agent's pending batch: realtime call
   * lifecycle for the agent, and channel messages that arrive while it is on a call
   * (injected into the call instead of starting a text turn).
   */
  function consume(agent: Agent, events: Event[]): Event[] {
    const remaining: Event[] = [];
    for (const event of events) {
      const payload = payloadOf(event);
      if (event.type === "voice.realtime.call.started") {
        if (payload.targetAgentName === agent.name) {
          track(attach(agent, event), "realtime.attach_failed");
        }
        continue;
      }
      if (event.type === "voice.realtime.call.ended") {
        const call = calls.get(String(payload.callId ?? ""));
        if (call && call.agent.name === agent.name && event.source !== `agent:${agent.name}`) {
          track(hangup(call, String(payload.reason ?? "ended"), true), "realtime.hangup_failed");
        }
        continue;
      }
      if (event.channelId && event.type === "message.created" && hasActiveCall(agent.name, event.channelId)) {
        if (!isRealtimeVoiceMessage(event) && event.source !== `agent:${agent.name}`) {
          for (const call of callsFor(agent.name, event.channelId)) {
            track(injectChannelMessage(call, event), "realtime.inject_failed");
          }
        }
        continue;
      }
      remaining.push(event);
    }
    return remaining;
  }

  async function endAgentCalls(agentName: string, reason = "agent_stopped") {
    await Promise.all(callsFor(agentName).map((call) => hangup(call, reason, false)));
  }

  async function endAll(reason = "runner_shutdown") {
    await Promise.all([...calls.values()].map((call) => hangup(call, reason, false)));
  }

  async function idle() {
    while (inFlight.size > 0) await Promise.all([...inFlight]);
  }

  return {
    consume,
    hasActiveCall,
    endAgentCalls,
    endAll,
    idle,
    activeCallIds: () => [...calls.keys()],
  };
}
