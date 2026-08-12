/**
 * The tab dies while /start is still in flight.
 *
 * This is the one moment where losing the connection costs SillyTavern the
 * whole reply rather than a resumable stream: the relay has the job and is
 * generating, but the browser never learned its id, so nothing on the page is
 * holding it and ST sees a failed request.
 *
 * That is exactly what recovery exists for, and the page load that made the
 * doomed request is the same one that has to pick the job back up — it was
 * never destroyed, only disconnected. So "a job this page started" cannot on
 * its own be the reason to leave a job alone; only "a job this page is still
 * asking for, or already streaming" can be.
 *
 * Modelled by rejecting the /start promise the extension is waiting on, without
 * touching the request the relay is serving — which is what a dropped socket
 * looks like from inside the page.
 *
 * Needs the fake upstream and an ST with the plugin loaded, same as relay-test.
 */
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const USER = process.env.ST_USER ?? 'default-user';
const RELAY = `${ST}/api/plugins/st-relay`;
const UPSTREAM = 'http://127.0.0.1:9911';
const CHAT_ID = `start-failure-test-chat-${Date.now()}`;

/** Long enough that /start is still pending when the socket "dies". */
const HEADER_DELAY_MS = 4000;
const KILL_START_AFTER_MS = 1000;

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

/** When set, /start rejects for the caller while the relay carries on serving it. */
let killStart = false;

globalThis.fetch = (input, init = {}) => {
    const url = typeof input === 'string' && input.startsWith('/') ? `${ST}${input}` : input;
    const merged = { ...(init.headers ?? {}) };
    if (!Object.keys(merged).some(h => h.toLowerCase() === 'cookie')) {
        merged['Cookie'] = cookie;
    }

    const request = nodeFetch(url, { ...init, headers: merged });

    if (killStart && String(url).includes('/st-relay/start')) {
        // Deliberately not aborting the underlying request: the point is that the
        // relay keeps the job while the page loses its answer.
        request.catch(() => {});
        return Promise.race([
            request,
            delay(KILL_START_AFTER_MS).then(() => {
                throw new TypeError('Load failed');
            }),
        ]);
    }

    return request;
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
            prompt: 'start failure test',
            max_tokens: 64,
        }),
    });
}

async function jobsForThisChat() {
    const response = await nodeFetch(`${RELAY}/jobs`, { headers: headers() });
    const { jobs = [] } = await response.json();
    return jobs.filter(job => job.meta?.chatId === CHAT_ID);
}

async function until(predicate, budgetMs = 25000, step = 250) {
    const deadline = Date.now() + budgetMs;
    while (Date.now() < deadline) {
        if (await predicate()) return true;
        await delay(step);
    }
    return false;
}

/* ---------------------------------------------------------------------- tests */

await import(pathToFileURL(EXTENSION).href);
check('extension loaded and installed the fetch shim', globalThis.fetch.__stBackgroundStream === true);

await delay(3500);

await configureUpstream(HEADER_DELAY_MS);
killStart = true;

let failed = null;
try {
    await generate();
} catch (error) {
    failed = error;
}
killStart = false;

check('the generation failed for SillyTavern, as a dropped socket would make it',
    failed !== null, failed ? failed.constructor.name : 'it resolved');

const orphaned = await until(async () => (await jobsForThisChat()).length === 1, 10000);
check('the relay kept generating even though the page lost the answer', orphaned,
    `${(await jobsForThisChat()).length} job(s)`);

const [job] = await jobsForThisChat();
check('the orphaned job is stamped with the page load that started it',
    Boolean(job?.meta?.clientId), job?.meta?.clientId ? 'clientId present' : 'no clientId');

// Nothing on the page is holding this job, so recovery has to take it — even
// though this very page load is the one that asked for it.
fireVisibilityChange();

const recovered = await until(async () =>
    ctx.chat.some(m => m?.extra?.st_background_stream_jobId === job?.jobId), 25000);
check('the page recovers the reply it never received', recovered,
    `${ctx.chat.length} message(s) in chat`);

const settled = await until(async () =>
    ctx.chat.find(m => m?.extra?.st_background_stream_jobId === job?.jobId)?.mes === EXPECTED, 10000);
check('the recovered reply is complete', settled);

check('only one message was created for it',
    ctx.chat.filter(m => m?.extra?.st_background_stream_jobId === job?.jobId).length === 1,
    `${ctx.chat.length} message(s)`);

// Which path took it matters. Adopting it live shows the reply arriving as it
// generates; waiting for `done` means staring at a failed generation until it
// finishes, which for a reasoning model is the whole run.
check('it was adopted live rather than waited out until the job finished',
    toasts.some(t => /picking up/i.test(String(t))),
    `toasts: ${toasts.join(' | ') || '(none)'}`);

await configureUpstream(0);

const bad = results.filter(r => !r.ok);
console.log(`\n${results.length - bad.length}/${results.length} checks passed`);
process.exit(bad.length === 0 ? 0 : 1);
