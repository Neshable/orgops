import type { Hono } from "hono";

import type { VoiceTranscriber } from "../voice-transcriber";
import type { RealtimeVoice } from "../voice-realtime";
import type { RequestUser } from "./access";

type VoiceDeps = {
  jsonResponse: (c: any, data: unknown, status?: number) => Response;
  requireAuth: (c: any, next: any) => Response | Promise<Response | void> | void;
  voiceTranscriber: VoiceTranscriber;
  realtimeVoice: RealtimeVoice;
};

function readString(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

export function registerVoiceRoutes(app: Hono<any>, deps: VoiceDeps) {
  const { jsonResponse, requireAuth, voiceTranscriber, realtimeVoice } = deps;

  app.get("/api/voice/config", requireAuth, (c) => {
    const { enabled, modelId } = voiceTranscriber.config();
    const realtime = realtimeVoice.config();
    return jsonResponse(c, {
      transcription: { enabled, modelId },
      realtime: { enabled: realtime.enabled, provider: realtime.provider },
    });
  });

  app.post("/api/voice/transcribe", requireAuth, async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as { fileId?: unknown };
    const fileId = typeof body.fileId === "string" ? body.fileId.trim() : "";
    if (!fileId) return jsonResponse(c, { error: "fileId is required" }, 400);
    try {
      const result = await voiceTranscriber.transcribeFile(fileId);
      return jsonResponse(c, result);
    } catch (error) {
      return jsonResponse(
        c,
        { error: error instanceof Error ? error.message : String(error) },
        502,
      );
    }
  });

  app.post("/api/voice/realtime/calls", requireAuth, async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as Record<string, unknown>;
    const request = {
      channelId: readString(body.channelId),
      agentName: readString(body.agentName),
      sessionId: readString(body.sessionId),
      sdp: typeof body.sdp === "string" ? body.sdp : "",
    };
    if (!request.channelId || !request.agentName || !request.sessionId || !request.sdp.trim()) {
      return jsonResponse(c, { error: "channelId, agentName, sessionId and sdp are required" }, 400);
    }
    const user = (c as any).get("user") as RequestUser | undefined;
    const result = await realtimeVoice.createCall(user, request);
    if (!result.ok) return jsonResponse(c, { error: result.error }, result.status);
    return jsonResponse(c, {
      sdp: result.sdp,
      callId: result.callId,
      model: result.model,
      agentName: result.agentName,
    });
  });
}
