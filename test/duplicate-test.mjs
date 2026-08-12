/**
 * A generation this page ran itself must never come back as a recovered message.
 *
 * The live path and the recovery path both end up writing the same reply, and
 * only one of them is allowed to. Once discovery started polling rather than
 * checking once at startup, every ordinary generation — including a swipe —
 * became a job sitting on the relay as `done` and unconsumed, which discovery
 * would then insert underneath the reply SillyTavern had already rendered.
 *
 * The generation here goes through the fetch shim exactly as SillyTavern's does,
 * and the body is drained to completion, so the page has the whole reply. After
 * that, no amount of discovery may put anything in the chat.
 *
 * Needs the fake upstream and an ST with the plugin loaded, same as relay-test.
 */
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const USER = process.env.ST_USER ?? 'default-user';
const RELAY = `${ST}/api/plugins/st-relay`;
const CHAT_ID = `duplicate-test-chat-${Date.now()}`;

const ST_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const EXTENSION_DIR = process.env.ST_EXTENSION_DIR
    ?? path.join(ST_ROOT, 'data', USER, 'extensions', 'SillyTavern-BackgroundStreaming');
const EXTENSION = path.join(EXTENSION_DIR, 'index.js');

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

// One context for the whole run, so `chat` is the array the extension would have
// pushed into. An empty one at the end is the assertion.
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

function payload() {
    return {
        api_type: 'ooba',
        api_server: 'http://127.0.0.1:9911',
        stream: true,
        prompt: 'duplicate test',
        max_tokens: 64,
    };
}

/** Drive one generation the way SillyTavern does, and read it to the end. */
async function generateAndDrain() {
    const response = await globalThis.fetch('/api/backends/text-completions/generate', {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(payload()),
    });

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

/** Poll until `predicate` holds or the budget runs out. */
async function until(predicate, budgetMs = 15000, step = 250) {
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

// Discovery starts on the extension's own 3s timer; let it be running before the
// generation so the two genuinely overlap, as they do in a browser.
await delay(3500);

const reply = await generateAndDrain();
check('the live path delivered the whole reply to the caller',
    (reply.match(/tok\d+ /g) ?? []).length === 20, `${(reply.match(/tok\d+ /g) ?? []).length}/20 tokens`);

const [job] = await jobsForThisChat();
check('the generation left exactly one job on the relay', Boolean(job), `${(await jobsForThisChat()).length} job(s)`);

const done = await until(async () => (await jobsForThisChat())[0]?.status === 'done');
check('the job finished on the relay', done);

const consumed = await until(async () => Boolean((await jobsForThisChat())[0]?.consumedAt), 5000);
check('the live path marked its own job consumed', consumed,
    'otherwise another tab or a later reload re-inserts a reply this page already has');

// The bug reproduces here: this is the discovery pass that used to append a copy
// of the reply as a new message.
fireVisibilityChange();
await delay(2000);
fireVisibilityChange();
await delay(2000);

check('discovery added nothing to the chat for a generation this page ran',
    ctx.chat.length === 0, `${ctx.chat.length} message(s) inserted`);

check('the user was not told anything was recovered',
    !toasts.some(t => /recover|picking up/i.test(String(t))), toasts.join(' | '));

/* ------------------------------------------- the same, for a swipe-shaped pair */

// A swipe is just a second generation in the same chat. Two jobs now sit on the
// relay for one chat, which is the shape that made the duplicate obvious.
await generateAndDrain();
await until(async () => (await jobsForThisChat()).every(j => j.consumedAt), 10000);

fireVisibilityChange();
await delay(2000);

const all = await jobsForThisChat();
check('a re-roll in the same chat also leaves nothing behind',
    ctx.chat.length === 0 && all.length === 2 && all.every(j => j.consumedAt),
    `${ctx.chat.length} message(s), ${all.filter(j => j.consumedAt).length}/${all.length} consumed`);

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
