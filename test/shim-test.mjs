/**
 * Drives the actual UI extension file in Node against the live relay, with just
 * enough of a browser stubbed in. Validates the part that cannot be checked from
 * the server side: the fetch shim, the reconnect/resume loop, and abort wiring.
 */
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const USER = process.env.ST_USER ?? 'default-user';

// The companion UI extension lives in its own repository, so its location is
// not derivable from this one. Default to where ST's extension installer puts
// it; override with ST_EXTENSION_DIR when running against a working copy.
// test/ -> <this plugin>/ -> plugins/ -> <SillyTavern root>
const ST_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const EXTENSION_DIR = process.env.ST_EXTENSION_DIR
    ?? path.join(ST_ROOT, 'data', USER, 'extensions', 'SillyTavern-BackgroundStreaming');
const EXTENSION = path.join(EXTENSION_DIR, 'index.js');

const results = [];
function check(name, ok, detail = '') {
    results.push({ name, ok });
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

/* --------------------------------------------------------------- browser stub */

const csrfResponse = await fetch(`${ST}/csrf-token`);
const cookie = (csrfResponse.headers.getSetCookie?.() ?? []).map(c => c.split(';')[0]).join('; ');
const { token } = await csrfResponse.json();

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
globalThis.toastr = { info() {}, success() {}, warning() {}, error() {} };
globalThis.SillyTavern = {
    getContext: () => ({
        getRequestHeaders: () => ({ 'Content-Type': 'application/json', 'X-CSRF-Token': token, 'Cookie': cookie }),
        getCurrentChatId: () => 'shim-test-chat',
        name2: 'TestChar',
        chat: [],
        extensionSettings: {},
        saveSettingsDebounced() {},
        eventSource: { on() {} },
        eventTypes: {},
    }),
};

// Node's fetch ignores a relative URL, and ST needs the session cookie on every
// hop, so resolve and re-inject both here — the browser does this natively.
const nodeFetch = globalThis.fetch;
globalThis.fetch = (input, init = {}) => {
    const url = typeof input === 'string' && input.startsWith('/') ? `${ST}${input}` : input;
    const headers = { ...(init.headers ?? {}) };
    if (!Object.keys(headers).some(h => h.toLowerCase() === 'cookie')) {
        headers['Cookie'] = cookie;
    }
    return nodeFetch(url, { ...init, headers });
};

await import(pathToFileURL(EXTENSION).href);
check('extension module loaded and installed the fetch shim', globalThis.fetch.__stBackgroundStream === true);

/* -------------------------------------------------------------------- helpers */

const PAYLOAD = {
    api_type: 'ooba',
    api_server: 'http://127.0.0.1:9911',
    stream: true,
    prompt: 'shim test',
    max_tokens: 64,
};

async function probe() {
    return (await nodeFetch('http://127.0.0.1:9911/_probe')).json();
}

function generate(signal) {
    // Exactly the shape SillyTavern's own generation call has.
    return globalThis.fetch('/api/backends/text-completions/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': token, 'Cookie': cookie },
        body: JSON.stringify(PAYLOAD),
        signal,
    });
}

function fireVisibilityChange() {
    for (const fn of listeners.get('visibilitychange') ?? []) fn();
}

/* ---------------------------------------------------------------------- tests */

async function testTransparentResume() {
    const before = await probe();
    const response = await generate();
    check('shim returns a streaming Response with upstream status', response.status === 200 && Boolean(response.body));

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = '';
    let interrupted = false;

    while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        text += decoder.decode(value, { stream: true });

        // Once a few tokens are in, simulate the tab waking from suspension. The
        // shim tears down its live connection and must resume from its offset.
        if (!interrupted && (text.match(/tok\d+ /g) ?? []).length >= 4) {
            interrupted = true;
            fireVisibilityChange();
        }
    }

    check('forced mid-stream reconnect happened', interrupted);

    const tokens = [...text.matchAll(/tok(\d+) /g)].map(m => Number(m[1]));
    const expected = Array.from({ length: 20 }, (_, i) => i);
    check('stream delivered to the caller is complete, ordered, and gap-free',
        JSON.stringify(tokens) === JSON.stringify(expected),
        `${tokens.length}/20 tokens`);

    const after = await probe();
    check('the reconnect did not restart or abort the upstream generation',
        after.aborted === before.aborted && after.requests === before.requests + 1,
        `requests +${after.requests - before.requests}, aborts +${after.aborted - before.aborted}`);
}

async function testAbortPropagates() {
    const before = await probe();
    const controller = new AbortController();
    const response = await generate(controller.signal);
    const reader = response.body.getReader();

    let seen = 0;
    while (seen < 2) {
        const { done, value } = await reader.read();
        if (done) break;
        seen += (new TextDecoder().decode(value).match(/tok\d+ /g) ?? []).length;
    }

    // This is what pressing Stop does.
    controller.abort();
    await new Promise(resolve => setTimeout(resolve, 1200));

    const after = await probe();
    check('aborting the caller signal cancels the upstream generation',
        after.aborted === before.aborted + 1,
        `upstream aborts ${before.aborted} -> ${after.aborted}`);
}

async function testPassthrough() {
    const response = await globalThis.fetch('/csrf-token');
    check('non-generation requests pass through untouched', response.ok);
}

await testTransparentResume();
await testAbortPropagates();
await testPassthrough();

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
