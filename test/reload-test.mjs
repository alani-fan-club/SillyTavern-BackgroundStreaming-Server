/**
 * The worst case: iOS discards the page outright, mid-generation.
 *
 * Nothing survives on the client — no stream to resume, no in-memory job id, no
 * pending fetch to hand a Response back to. All that is left is a job on the
 * relay and a chat on disk. A page loading into that state has to find the job
 * and finish the message itself.
 *
 * Modelled by creating the job the way the destroyed page would have, never
 * reading it, and only then loading the extension — so the module genuinely has
 * no prior knowledge of the job, which is the whole point.
 *
 * Needs the fake upstream and an ST with the plugin loaded, same as relay-test.
 */
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const USER = process.env.ST_USER ?? 'default-user';
const RELAY = `${ST}/api/plugins/st-relay`;
const CHAT_ID = `reload-test-chat-${Date.now()}`;

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
    info: (message) => toasts.push(message),
    success: (message) => toasts.push(message),
    warning: (message) => toasts.push(message),
    error: (message) => toasts.push(message),
};

// One context object for the whole run, unlike shim-test's fresh-object-per-call
// stub: `chat` has to be the same array the extension pushed into, or nothing it
// writes would be observable here.
let updateCalls = 0;
let saveCalls = 0;
const ctx = {
    getRequestHeaders: headers,
    getCurrentChatId: () => CHAT_ID,
    name2: 'TestChar',
    chat: [],
    extensionSettings: {},
    saveSettingsDebounced() {},
    addOneMessage: async () => {},
    updateMessageBlock: () => { updateCalls += 1; },
    saveChat: async () => { saveCalls += 1; },
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

function payload(stream = true) {
    return {
        api_type: 'ooba',
        api_server: 'http://127.0.0.1:9911',
        stream,
        prompt: 'reload test',
        max_tokens: 64,
    };
}

async function probe() {
    return (await nodeFetch('http://127.0.0.1:9911/_probe')).json();
}

/** Start a job the way the page that later died would have, and abandon it. */
async function startAbandonedJob() {
    const response = await nodeFetch(`${RELAY}/start`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
            target: '/api/backends/text-completions/generate',
            payload: payload(),
            meta: { chatId: CHAT_ID, characterName: 'TestChar', startedAt: new Date().toISOString() },
        }),
    });
    if (!response.ok) {
        throw new Error(`/start returned ${response.status} ${await response.text()}`);
    }
    return response.json();
}

async function statusOf(jobId) {
    const response = await nodeFetch(`${RELAY}/status?jobId=${encodeURIComponent(jobId)}`, { headers: headers() });
    return response.ok ? response.json() : null;
}

function messageFor(jobId) {
    return ctx.chat.find(m => m?.extra?.st_background_stream_jobId === jobId) ?? null;
}

/** Poll until `predicate` holds or the budget runs out. */
async function until(predicate, budgetMs = 20000, step = 250) {
    const deadline = Date.now() + budgetMs;
    while (Date.now() < deadline) {
        if (await predicate()) return true;
        await delay(step);
    }
    return false;
}

/* ------------------------------------------------- the page dies mid-generation */

const before = await probe();
const running = await startAbandonedJob();
check('a generation was left running with nobody watching it',
    running.statusCode === 200, `job ${running.jobId}`);

// The page reloads. The extension has never seen this job, and its own startup
// timer is what kicks discovery off, exactly as in a browser.
await import(pathToFileURL(EXTENSION).href);
check('extension loaded into a page that knows nothing about the job',
    globalThis.fetch.__stBackgroundStream === true);

const stillRunning = (await statusOf(running.jobId))?.status;
check('the job was still generating when the page came back',
    stillRunning === 'running' || stillRunning === 'starting', `status=${stillRunning}`);

const adopted = await until(async () => messageFor(running.jobId) !== null, 12000);
check('the reloaded page adopted the running job and opened a message', adopted);

check('the user was told a generation was being picked up',
    toasts.some(t => /picking up/i.test(String(t))));

const grew = await until(async () => updateCalls >= 2, 15000);
check('the message filled in progressively rather than appearing at once',
    grew, `${updateCalls} in-place updates`);

const completed = await until(async () => (await statusOf(running.jobId))?.status === 'done', 20000);
check('the generation ran to completion', completed);

const settled = await until(async () => messageFor(running.jobId)?.mes === EXPECTED, 8000);
check('the recovered message holds the complete reply', settled,
    messageFor(running.jobId)?.mes ? `${messageFor(running.jobId).mes.length} chars` : 'empty');

check('only one message was created for the job',
    ctx.chat.filter(m => m?.extra?.st_background_stream_jobId === running.jobId).length === 1,
    `${ctx.chat.length} message(s) in chat`);

const saved = await until(async () => saveCalls > 0, 5000);
check('the recovered chat was saved', saved);

const consumed = await until(async () => Boolean((await statusOf(running.jobId))?.consumedAt), 5000);
check('the job was consumed so it is not offered again', consumed);

const after = await probe();
check('adopting a job never aborted or restarted it upstream',
    after.aborted === before.aborted && after.requests === before.requests + 1,
    `requests +${after.requests - before.requests}, aborts +${after.aborted - before.aborted}`);

/* ----------------------------------------------- and again, for a finished job */

// The other half of a full unload: the reply landed while the page was gone.
const finishedJob = await startAbandonedJob();
const ready = await until(async () => (await statusOf(finishedJob.jobId))?.status === 'done', 20000);
check('a second generation finished while nothing was watching', ready);

fireVisibilityChange();

const inserted = await until(async () => messageFor(finishedJob.jobId)?.mes === EXPECTED, 10000);
check('a finished job is inserted automatically on the next check', inserted);

// Discovery runs on a timer, so anything that is not idempotent duplicates.
fireVisibilityChange();
await delay(1500);
check('re-checking does not insert the same reply twice',
    ctx.chat.filter(m => m?.extra?.st_background_stream_jobId === finishedJob.jobId).length === 1);

check('both recoveries are marked as read on the relay',
    Boolean((await statusOf(finishedJob.jobId))?.consumedAt));

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
