/**
 * A page must not adopt a job it started itself.
 *
 * The relay registers a job before it has upstream headers, and /start does not
 * resolve until it has them — so between those two moments the job is listed by
 * /jobs, still running, while the page that asked for it has not yet been told
 * its id. A discovery pass landing in that window sees a running job nobody
 * claims and adopts it. SillyTavern then streams the reply into its own message
 * and the extension streams the same bytes into a second one, both at once.
 *
 * The window is however long the provider takes to send headers: milliseconds
 * for a fast one, tens of seconds for a reasoning model. The fake upstream is
 * told to hold headers here so the window is wide enough to aim at.
 *
 * Needs the fake upstream and an ST with the plugin loaded, same as relay-test.
 */
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const USER = process.env.ST_USER ?? 'default-user';
const RELAY = `${ST}/api/plugins/st-relay`;
const UPSTREAM = 'http://127.0.0.1:9911';
const CHAT_ID = `self-adopt-test-chat-${Date.now()}`;

/** Long enough to fire several discovery passes inside the window. */
const HEADER_DELAY_MS = 3000;

const ST_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const EXTENSION_DIR = process.env.ST_EXTENSION_DIR
    ?? path.join(ST_ROOT, 'data', USER, 'extensions', 'SillyTavern-BackgroundStreaming');
const EXTENSION = path.join(EXTENSION_DIR, 'index.js');

const EXPECTED = Array.from({ length: 20 }, (_, i) => `tok${i} `).join('').trim();

const results = [];
function check(name, ok, detail = '') {
    results.push({ name, ok });
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

/* --------------------------------------------------------------- browser stub */

const nodeFetch = globalThis.fetch;

const csrfResponse = await nodeFetch(`${ST}/csrf-token`);
const cookie = (csrfResponse.headers.getSetCookie?.() ?? []).map(c => c.split(';')[0]).join('; ');
const { token } = await csrfResponse.json();

const headers = () => ({ 'Content-Type': 'application/json', 'X-CSRF-Token': token, 'Cookie': cookie });

const listeners = new Map();
globalThis.document = {
    visibilityState: 'visible',
    getElementById: () => null,
    addEventListener: (type, fn) => {
        if (!listeners.has(type)) listeners.set(type, new Set());
        listeners.get(type).add(fn);
    },
    removeEventListener: (type, fn) => listeners.get(type)?.delete(fn),
    createElement: () => ({ classList: { add() {} }, querySelector: () => null, append() {}, set innerHTML(_) {} }),
};
globalThis.location = { origin: ST };

const toasts = [];
globalThis.toastr = {
    info: message => toasts.push(message),
    success: message => toasts.push(message),
    warning: message => toasts.push(message),
    error: message => toasts.push(message),
};

const ctx = {
    getRequestHeaders: headers,
    getCurrentChatId: () => CHAT_ID,
    name2: 'TestChar',
    chat: [],
    extensionSettings: {},
    saveSettingsDebounced() {},
    addOneMessage: async () => {},
    updateMessageBlock: () => {},
    saveChat: async () => {},
    eventSource: { on() {} },
    eventTypes: {},
};
globalThis.SillyTavern = { getContext: () => ctx };

globalThis.fetch = (input, init = {}) => {
    const url = typeof input === 'string' && input.startsWith('/') ? `${ST}${input}` : input;
    const merged = { ...(init.headers ?? {}) };
    if (!Object.keys(merged).some(h => h.toLowerCase() === 'cookie')) {
        merged['Cookie'] = cookie;
    }
    return nodeFetch(url, { ...init, headers: merged });
};

function fireVisibilityChange() {
    for (const fn of listeners.get('visibilitychange') ?? []) fn();
}

/* -------------------------------------------------------------------- helpers */

async function configureUpstream(ms) {
    await nodeFetch(`${UPSTREAM}/_config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headerDelayMs: ms }),
    });
}

function generate() {
    return globalThis.fetch('/api/backends/text-completions/generate', {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
            api_type: 'ooba',
            api_server: UPSTREAM,
            stream: true,
            prompt: 'self adopt test',
            max_tokens: 64,
        }),
    });
}

async function drain(response) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = '';
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        text += decoder.decode(value, { stream: true });
    }
    return text;
}

async function jobsForThisChat() {
    const response = await nodeFetch(`${RELAY}/jobs`, { headers: headers() });
    const { jobs = [] } = await response.json();
    return jobs.filter(job => job.meta?.chatId === CHAT_ID);
}

/* ---------------------------------------------------------------------- tests */

await import(pathToFileURL(EXTENSION).href);
check('extension loaded and installed the fetch shim', globalThis.fetch.__stBackgroundStream === true);

// Let the extension's own startup finish, so discovery is live before the race.
await delay(3500);

await configureUpstream(HEADER_DELAY_MS);

// Send, and do not wait: the point is what happens while /start is in flight.
const pending = generate();

// The tab coming back to the foreground is what fires a discovery pass in a
// browser. Fire several, all of them inside the window.
const seenWhileStarting = [];
for (let i = 0; i < 6; i++) {
    await delay(400);
    seenWhileStarting.push(...(await jobsForThisChat()).map(j => j.status));
    fireVisibilityChange();
}

check('the job was visible to discovery before /start resolved',
    seenWhileStarting.some(status => status === 'starting' || status === 'running'),
    `statuses seen: ${[...new Set(seenWhileStarting)].join(', ') || 'none'}`);

const reply = await drain(await pending);
check('SillyTavern still received the whole reply',
    (reply.match(/tok\d+ /g) ?? []).length === 20, `${(reply.match(/tok\d+ /g) ?? []).length}/20 tokens`);

await delay(2000);

check('the page did not adopt its own in-flight job',
    ctx.chat.length === 0,
    ctx.chat.length ? `${ctx.chat.length} duplicate message(s), first is ${ctx.chat[0]?.mes?.length ?? 0} chars` : '');

check('nothing announced itself as a recovery',
    !toasts.some(t => /picking up|recover/i.test(String(t))), toasts.join(' | '));

// Whatever happened, the reply must not have been generated twice upstream.
const jobs = await jobsForThisChat();
check('exactly one job was created for the one generation', jobs.length === 1, `${jobs.length} job(s)`);

/* ------------------------------- recovery of somebody else's job still works */

// The guard must key on this page load, not on "a job exists". A job started
// without the extension is exactly what a destroyed page leaves behind, and it
// still has to be picked up.
await configureUpstream(0);

const orphan = await (await nodeFetch(`${RELAY}/start`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify({
        target: '/api/backends/text-completions/generate',
        payload: { api_type: 'ooba', api_server: UPSTREAM, stream: true, prompt: 'orphan', max_tokens: 64 },
        meta: { chatId: CHAT_ID, characterName: 'TestChar', startedAt: new Date().toISOString() },
    }),
})).json();

fireVisibilityChange();

const deadline = Date.now() + 20000;
let recovered = null;
while (Date.now() < deadline && !recovered) {
    await delay(250);
    recovered = ctx.chat.find(m => m?.extra?.st_background_stream_jobId === orphan.jobId) ?? null;
}

check('a job this page did not start is still recovered', Boolean(recovered));

const settledDeadline = Date.now() + 8000;
while (Date.now() < settledDeadline && recovered?.mes !== EXPECTED) {
    await delay(250);
}
check('the recovered reply is complete', recovered?.mes === EXPECTED,
    `${recovered?.mes?.length ?? 0} chars`);

await configureUpstream(0);

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
