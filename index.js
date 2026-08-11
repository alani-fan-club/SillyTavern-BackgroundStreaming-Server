/**
 * st-relay — SillyTavern Server Plugin
 *
 * Keeps streaming generations alive when the client goes away.
 *
 * Normally the browser holds the SSE connection to /api/backends/.../generate
 * directly, and every backend registers `request.socket.on('close', () => abort())`.
 * When a mobile browser suspends a backgrounded tab the socket dies and the
 * upstream generation is genuinely cancelled at the provider.
 *
 * This plugin inserts itself as a buffering hop. It re-issues the *same* request
 * over loopback to SillyTavern's own endpoint, forwarding the caller's session
 * cookie and CSRF token untouched. The socket that ST's endpoint sees then
 * belongs to this plugin, which never goes away, so nothing gets aborted. The
 * response bytes are buffered and replayed to any number of successive client
 * connections, resumable by byte offset.
 *
 * Deliberately knows nothing about any LLM provider: the payload and the
 * response body are opaque. That is what keeps it working across ST updates.
 *
 * ST server plugins export: init(router), exit(), info
 */

const http = require("node:http");
const https = require("node:https");
const crypto = require("node:crypto");

const info = {
  id: "st-relay",
  name: "Detached Generation Relay",
  description: "Buffers generations server-side so a suspended browser tab does not abort them.",
};

/**
 * Endpoints the relay is allowed to call back into. Strict allowlist: without it
 * this route would be an authenticated SSRF proxy into arbitrary ST internals.
 */
const ALLOWED_TARGETS = new Set([
  "/api/backends/chat-completions/generate",
  "/api/backends/text-completions/generate",
  "/api/backends/kobold/generate",
  "/api/backends/koboldhorde/generate",
  "/api/novelai/generate",
]);

/** Headers that must never be forwarded — see notes in buildForwardHeaders. */
const STRIPPED_HEADERS = new Set([
  "host",
  "connection",
  "content-length",
  "transfer-encoding",
  "accept-encoding",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-real-ip",
  "cf-connecting-ip",
]);

const MAX_JOB_BYTES = 16 * 1024 * 1024;
const MAX_JOBS = 64;

/**
 * Cap on a single user's simultaneously *running* jobs.
 *
 * MAX_JOBS alone does not bound memory: sweep() can only evict jobs that have
 * finished, so without this a client that keeps starting generations without
 * finishing them grows the buffer without limit, at up to MAX_JOB_BYTES each.
 * Per-user rather than global so one busy account cannot lock everyone else out.
 */
const MAX_LIVE_JOBS_PER_USER = 8;

/**
 * How long a finished job stays available for recovery. This is the limit on
 * walking away from a generation and coming back to it: leave for longer than
 * this and the buffered reply has been swept.
 *
 * Nothing bounds how long a *running* job may take. Only the headers phase of
 * the loopback call is on a timer, so a model that thinks for an hour is fine.
 */
const RETENTION_MS = 60 * 60 * 1000;
const SWEEP_INTERVAL_MS = 60 * 1000;
const HEARTBEAT_MS = 15 * 1000;

/**
 * How long to wait for ST to send *response headers* on the loopback call. Only
 * covers the headers phase — once the response starts, a generation may then
 * take as long as it likes, including long silences from a thinking model.
 */
const UPSTREAM_HEADERS_TIMEOUT_MS = 60 * 1000;

const TERMINAL = new Set(["done", "error", "aborted"]);

/** @type {Map<string, object>} */
const jobs = new Map();

let sweepTimer = null;

/* ------------------------------------------------------------------ helpers */

function isTerminal(job) {
  return TERMINAL.has(job.status);
}

/**
 * Did the loopback hop get turned away before it reached the generation code?
 *
 * ST's whitelist, basic auth and login middleware all answer with an HTML error
 * page. The generation endpoints answer with JSON or an event stream and never
 * with HTML, so an HTML 4xx is the infrastructure talking, not the provider.
 * Worth telling apart: a provider's own 401 is a real answer the user needs to
 * see, while this means the relay itself is not usable in this deployment and
 * the caller is better off generating directly.
 */
function isBlockedByMiddleware(job) {
  return job.statusCode >= 400
    && job.statusCode < 500
    && String(job.contentType).toLowerCase().includes("text/html");
}

/**
 * The user a job belongs to. Plugin routers mount after ST's session and
 * requireLogin middleware, so this is populated for every request that gets
 * here; the `?? null` is only for exotic configurations.
 */
function ownerOf(request) {
  return request.user?.profile?.handle ?? null;
}

/**
 * Look up a job, but only if it belongs to the caller. Job IDs are unguessable
 * UUIDs, but on a multi-user instance that is not by itself an access control
 * decision — one user's generated text must not be reachable from another's
 * session, and /jobs would otherwise enumerate everything on the server.
 */
function findOwnedJob(request, jobId) {
  const job = jobs.get(String(jobId ?? ""));
  return job && job.owner === ownerOf(request) ? job : null;
}

function describeJob(job) {
  return {
    jobId: job.id,
    target: job.target,
    status: job.status,
    statusCode: job.statusCode,
    contentType: job.contentType,
    byteLength: job.byteLength,
    truncated: job.truncated,
    error: job.error,
    createdAt: job.createdAt,
    finishedAt: job.finishedAt,
    consumedAt: job.consumedAt,
    listeners: job.listeners.size,
    meta: job.meta,
  };
}

/**
 * Build the header set for the loopback request.
 *
 * Forwards `cookie` (ST uses cookie-session, so the whole session travels in the
 * signed cookie and needs no server-side lookup) and `x-csrf-token` (csrf-sync
 * compares it against a value inside that same session).
 *
 * Deliberately does NOT forward the forwarded-IP headers. With
 * `enableForwardedWhitelist: true` the whitelist middleware re-checks
 * X-Real-IP / CF-Connecting-IP / X-Forwarded-For, so passing the remote client's
 * address along would make the loopback call fail the whitelist that the client
 * itself already passed.
 *
 * Nor `accept-encoding`. ST installs `compression()` globally and
 * `text/event-stream` is a compressible type, so forwarding the browser's
 * `gzip, br` would let ST compress the response. The bytes buffered here are
 * replayed verbatim, and nothing downstream re-declares a Content-Encoding, so
 * the client would be handed compressed bytes it never decodes.
 */
function buildForwardHeaders(request, bodyLength) {
  const headers = {};

  for (const [name, value] of Object.entries(request.headers)) {
    const lower = name.toLowerCase();
    if (STRIPPED_HEADERS.has(lower) || lower.startsWith(":")) {
      continue;
    }
    if (value !== undefined) {
      headers[lower] = value;
    }
  }

  headers["content-type"] = "application/json";
  headers["content-length"] = String(bodyLength);
  headers["accept"] = "text/event-stream, application/json;q=0.9, */*;q=0.8";
  headers["accept-encoding"] = "identity";
  return headers;
}

/** Connection-level failures worth retrying against the next candidate host. */
const CONNECT_ERRORS = new Set([
  "ECONNREFUSED",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EADDRNOTAVAIL",
  "ETIMEDOUT",
]);

/**
 * Addresses to try for the loopback call, best first.
 *
 * Real loopback has to come first, and not merely because it is the shortest
 * path. ST's whitelist middleware re-checks the source IP of this call, and the
 * default whitelist is loopback only. Dialing whatever address the request
 * happened to arrive on makes the relay present itself as a remote client, so it
 * only works where that address is already whitelisted. It is not: users
 * whitelist the addresses their *clients* connect from, not the ones their
 * server answers on. Any reachable interface that is not the LAN subnet breaks,
 * which covers VPN and overlay networks, additional NICs, and public addresses.
 * Loopback needs no whitelist entry in any deployment.
 *
 * The arrival address is still worth keeping as a fallback, for the case that
 * motivated looking at this at all: ST bound IPv6-only (`protocol.ipv4: false`)
 * or to one specific `listenAddress`, where 127.0.0.1 is not listening.
 */
function loopbackCandidates(request) {
  const address = request.socket.localAddress || "";
  // A v4 peer on a dual-stack socket is reported as ::ffff:127.0.0.1.
  const mapped = address.startsWith("::ffff:");
  const arrival = mapped ? address.slice("::ffff:".length) : address;
  const loopback = !mapped && request.socket.localFamily === "IPv6" ? "::1" : "127.0.0.1";

  return arrival && arrival !== loopback ? [loopback, arrival] : [loopback];
}

function appendChunk(job, chunk) {
  if (job.byteLength + chunk.length > MAX_JOB_BYTES) {
    job.truncated = true;
    finishJob(job, "error", `Response exceeded the ${MAX_JOB_BYTES} byte relay buffer limit`);
    destroyUpstream(job);
    return;
  }

  job.chunks.push(chunk);
  job.byteLength += chunk.length;
  notify(job);
}

function finishJob(job, status, error = null) {
  if (isTerminal(job)) {
    return;
  }
  job.status = status;
  job.error = error;
  job.finishedAt = Date.now();
  notify(job);
}

function destroyUpstream(job) {
  try {
    job.request?.destroy();
  } catch {
    // Already gone; nothing to do.
  }
}

function notify(job) {
  for (const listener of job.listeners) {
    try {
      listener();
    } catch (error) {
      console.error(`[st-relay] listener for job ${job.id} threw:`, error);
    }
  }
}

/**
 * Position a reader at an absolute byte offset within the job buffer.
 * Chunks before the offset are skipped whole; the one straddling it is
 * partially consumed.
 */
function makeCursor(job, fromOffset) {
  let index = 0;
  let base = 0;

  while (index < job.chunks.length && base + job.chunks[index].length <= fromOffset) {
    base += job.chunks[index].length;
    index += 1;
  }

  return { index, absolute: base, skip: Math.max(0, fromOffset - base) };
}

function writeEvent(response, event, data, id) {
  if (response.writableEnded) {
    return;
  }
  let frame = `event: ${event}\n`;
  if (id !== undefined) {
    frame += `id: ${id}\n`;
  }
  frame += `data: ${data}\n\n`;
  response.write(frame);
}

/* -------------------------------------------------------------- job lifecycle */

/**
 * Issue the loopback request and resolve once the upstream response headers are
 * in, so the caller can mirror the real status code back to the browser.
 */
function startUpstream(job, request, bodyBuffer) {
  return new Promise((resolve, reject) => {
    const isSecure = Boolean(request.socket.encrypted);
    const transport = isSecure ? https : http;
    const candidates = loopbackCandidates(request);
    const headers = buildForwardHeaders(request, bodyBuffer.length);

    let index = 0;
    let settled = false;
    let headersTimer = null;

    /** Resolve or reject exactly once, and stop the headers watchdog. */
    const settle = (finish, value) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(headersTimer);
      finish(value);
    };

    const attempt = () => {
      const host = candidates[index];

      const upstream = transport.request({
        host,
        port: request.socket.localPort,
        path: job.target,
        method: "POST",
        headers,
        // Loopback to ourselves. If ST is serving TLS it is very likely a
        // self-signed cert, and there is no trust decision to make on loopback.
        rejectUnauthorized: false,
      }, (response) => {
        const statusCode = response.statusCode ?? 0;
        const contentType = response.headers["content-type"] ?? "application/octet-stream";

        // Turned away by ST's own middleware rather than answered. The other
        // candidate may well be allowed where this one is not: deployments
        // whitelist loopback, or their LAN subnet, or both, and which one is
        // present is not knowable from here. Nothing has been buffered yet, so
        // trying the next address cannot duplicate a generation.
        if (!settled && isBlockedByMiddleware({ statusCode, contentType }) && index + 1 < candidates.length) {
          console.warn(`[st-relay] ${host} was rejected with HTTP ${statusCode} by SillyTavern, retrying via ${candidates[index + 1]}`);
          response.resume();
          index += 1;
          attempt();
          return;
        }

        job.statusCode = statusCode;
        job.contentType = contentType;
        job.status = "running";

        response.on("data", (chunk) => appendChunk(job, chunk));
        response.on("end", () => finishJob(job, "done"));
        response.on("aborted", () => finishJob(job, "error", "Upstream connection aborted"));
        response.on("error", (error) => finishJob(job, "error", error.message));

        settle(resolve);
      });

      upstream.on("error", (error) => {
        // Nothing was received yet, so moving to the next candidate cannot
        // duplicate a generation. Covers ST bound somewhere loopback is not.
        if (!settled && CONNECT_ERRORS.has(error.code) && index + 1 < candidates.length) {
          console.warn(`[st-relay] ${host} unreachable (${error.code}), retrying via ${candidates[index + 1]}`);
          index += 1;
          attempt();
          return;
        }

        finishJob(job, "error", error.message);
        settle(reject, error);
      });

      job.request = upstream;
      upstream.end(bodyBuffer);
    };

    // Without this, an ST endpoint that never replies leaves /start pending
    // forever, and the browser's generation hangs with no path to a fallback.
    headersTimer = setTimeout(() => {
      const error = new Error(`No response headers from ${job.target} within ${UPSTREAM_HEADERS_TIMEOUT_MS}ms`);
      finishJob(job, "error", error.message);
      settle(reject, error);
      destroyUpstream(job);
    }, UPSTREAM_HEADERS_TIMEOUT_MS);
    headersTimer.unref?.();

    attempt();
  });
}

function sweep() {
  const now = Date.now();

  for (const [id, job] of jobs) {
    if (isTerminal(job) && job.finishedAt && now - job.finishedAt > RETENTION_MS) {
      jobs.delete(id);
    }
  }

  if (jobs.size <= MAX_JOBS) {
    return;
  }

  const evictable = [...jobs.values()]
    .filter(isTerminal)
    .sort((a, b) => (a.finishedAt ?? 0) - (b.finishedAt ?? 0));

  for (const job of evictable) {
    if (jobs.size <= MAX_JOBS) {
      break;
    }
    jobs.delete(job.id);
  }
}

/**
 * Decide whether to accept another job, having first swept whatever can be
 * reclaimed. Returns null to accept, or a message explaining the refusal.
 *
 * Refusing is a good outcome: the extension treats a failed /start as "relay
 * unavailable" and issues the generation directly instead, so the user loses
 * background resumption for that one message rather than the message itself.
 */
function admissionRefusal(owner) {
  sweep();

  const live = [...jobs.values()].filter(job => !isTerminal(job));

  if (live.filter(job => job.owner === owner).length >= MAX_LIVE_JOBS_PER_USER) {
    return `You already have ${MAX_LIVE_JOBS_PER_USER} generations running through the relay`;
  }
  if (jobs.size >= MAX_JOBS) {
    return `The relay is holding its maximum of ${MAX_JOBS} jobs`;
  }
  return null;
}

/* ------------------------------------------------------------------- plugin */

async function init(router) {
  /**
   * Begin a detached generation. Resolves once upstream headers arrive.
   */
  router.post("/start", async (request, response) => {
    const target = String(request.body?.target ?? "");
    const payload = request.body?.payload;

    if (!ALLOWED_TARGETS.has(target)) {
      return response.status(400).json({ error: `Target '${target}' is not an allowed relay endpoint` });
    }
    if (payload === undefined || payload === null || typeof payload !== "object") {
      return response.status(400).json({ error: "Missing or non-object 'payload'" });
    }

    const owner = ownerOf(request);
    const refusal = admissionRefusal(owner);

    if (refusal) {
      console.warn(`[st-relay] refused a job for ${target}: ${refusal}`);
      return response.status(503).json({ error: `${refusal}. Try again once one finishes.` });
    }

    const job = {
      id: crypto.randomUUID(),
      owner,
      target,
      meta: request.body?.meta ?? {},
      status: "starting",
      statusCode: 0,
      contentType: "application/octet-stream",
      chunks: [],
      byteLength: 0,
      truncated: false,
      error: null,
      createdAt: Date.now(),
      finishedAt: null,
      consumedAt: null,
      request: null,
      listeners: new Set(),
    };

    jobs.set(job.id, job);

    try {
      await startUpstream(job, request, Buffer.from(JSON.stringify(payload), "utf-8"));
    } catch (error) {
      jobs.delete(job.id);
      console.error(`[st-relay] failed to start job for ${target}:`, error);
      return response.status(502).json({ error: `Relay loopback request failed: ${error.message}` });
    }

    // Report this as a relay failure rather than passing the error page on as if
    // it were the generation's answer. The extension reads a failed /start as
    // "relay unavailable" and sends the request directly, which costs background
    // resumption for this message instead of failing it outright.
    if (isBlockedByMiddleware(job)) {
      destroyUpstream(job);
      jobs.delete(job.id);
      console.error(`[st-relay] loopback to ${job.target} was rejected with HTTP ${job.statusCode} by SillyTavern itself. Check that this server's own address is allowed to reach it.`);
      return response.status(502).json({
        error: `Relay loopback rejected with HTTP ${job.statusCode} before reaching ${job.target}`,
      });
    }

    console.info(`[st-relay] job ${job.id} started (${target} -> ${job.statusCode})`);
    return response.json(describeJob(job));
  });

  /**
   * Replay a job from a byte offset, then follow it live. Reconnect as often as
   * needed; a dropped connection here never affects the job.
   */
  router.get("/stream", (request, response) => {
    const job = findOwnedJob(request, request.query.jobId);

    if (!job) {
      return response.status(404).json({ error: "Unknown or expired job" });
    }

    const requested = Number.parseInt(String(request.query.from ?? "0"), 10);
    const from = Number.isFinite(requested) && requested > 0 ? Math.min(requested, job.byteLength) : 0;

    response.status(200);
    response.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    response.setHeader("Cache-Control", "no-cache, no-transform");
    response.setHeader("Connection", "keep-alive");
    response.setHeader("X-Accel-Buffering", "no");
    response.flushHeaders?.();

    const cursor = makeCursor(job, from);
    let closed = false;

    writeEvent(response, "meta", JSON.stringify({
      jobId: job.id,
      status: job.status,
      statusCode: job.statusCode,
      contentType: job.contentType,
      byteLength: job.byteLength,
      resumedAt: from,
    }));

    const pump = () => {
      if (closed || response.writableEnded) {
        return;
      }

      while (cursor.index < job.chunks.length) {
        const chunk = job.chunks[cursor.index];
        const slice = cursor.skip > 0 ? chunk.subarray(cursor.skip) : chunk;
        const startOffset = cursor.absolute + cursor.skip;

        if (slice.length > 0) {
          writeEvent(response, "chunk", slice.toString("base64"), String(startOffset));
        }

        cursor.absolute += chunk.length;
        cursor.skip = 0;
        cursor.index += 1;
      }

      if (isTerminal(job)) {
        writeEvent(response, "end", JSON.stringify({
          status: job.status,
          error: job.error,
          byteLength: job.byteLength,
          truncated: job.truncated,
        }));
        cleanup();
        response.end();
      }
    };

    const heartbeat = setInterval(() => {
      if (!closed && !response.writableEnded) {
        response.write(": heartbeat\n\n");
      }
    }, HEARTBEAT_MS);

    function cleanup() {
      if (closed) {
        return;
      }
      closed = true;
      clearInterval(heartbeat);
      job.listeners.delete(pump);
    }

    job.listeners.add(pump);

    // The client vanishing is the whole scenario this plugin exists for.
    // Drop the listener and leave the job running.
    response.on("close", cleanup);

    pump();
  });

  /**
   * The only thing that cancels a generation. The UI extension calls this from
   * the abort signal of ST's own fetch, which fires on Stop but not on tab
   * suspension — that distinction is the entire point.
   */
  router.post("/abort", (request, response) => {
    const job = findOwnedJob(request, request.body?.jobId);

    if (!job) {
      return response.status(404).json({ error: "Unknown or expired job" });
    }

    destroyUpstream(job);
    finishJob(job, "aborted");
    console.info(`[st-relay] job ${job.id} aborted by client`);
    return response.json(describeJob(job));
  });

  /** Jobs a reloaded page might want to recover. Scoped to the calling user. */
  router.get("/jobs", (request, response) => {
    sweep();
    const owner = ownerOf(request);
    const owned = [...jobs.values()].filter(job => job.owner === owner);
    return response.json({ jobs: owned.map(describeJob) });
  });

  router.get("/status", (request, response) => {
    const job = findOwnedJob(request, request.query.jobId);
    if (!job) {
      return response.status(404).json({ error: "Unknown or expired job" });
    }
    return response.json(describeJob(job));
  });

  /** Full buffered body, for recovering a generation the client never saw. */
  router.get("/result", (request, response) => {
    const job = findOwnedJob(request, request.query.jobId);
    if (!job) {
      return response.status(404).json({ error: "Unknown or expired job" });
    }
    response.setHeader("Content-Type", job.contentType);
    return response.status(200).end(Buffer.concat(job.chunks));
  });

  /** Mark a job as dealt with so recovery stops offering it. */
  router.post("/consume", (request, response) => {
    const job = findOwnedJob(request, request.body?.jobId);
    if (!job) {
      return response.status(404).json({ error: "Unknown or expired job" });
    }
    job.consumedAt = Date.now();
    return response.json(describeJob(job));
  });

  router.get("/health", (request, response) => {
    const owner = ownerOf(request);
    return response.json({
      ok: true,
      version: require("./package.json").version,
      jobs: [...jobs.values()].filter(job => job.owner === owner).length,
      allowedTargets: [...ALLOWED_TARGETS],
    });
  });

  sweepTimer = setInterval(sweep, SWEEP_INTERVAL_MS);
  sweepTimer.unref?.();

  console.info("[st-relay] ready — detached generation relay mounted at /api/plugins/st-relay");
}

async function exit() {
  if (sweepTimer) {
    clearInterval(sweepTimer);
    sweepTimer = null;
  }

  for (const job of jobs.values()) {
    destroyUpstream(job);
  }

  jobs.clear();
}

module.exports = { info, init, exit };

/** Pure helpers whose behaviour is worth pinning down. See test/loopback-test.mjs. */
module.exports.__test = { loopbackCandidates, isBlockedByMiddleware };
