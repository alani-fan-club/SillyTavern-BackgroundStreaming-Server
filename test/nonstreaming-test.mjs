/**
 * Does any of this work with streaming turned off in SillyTavern?
 *
 * It is a fair thing to doubt, because "resume a stream by byte offset" sounds
 * like it needs a stream. But the bug being worked around is not about
 * streaming: ST aborts the provider request when the client socket closes, and
 * it registers that handler before it branches on `stream`. A non-streaming
 * generation is one long silence followed by one JSON body, so a tab suspended
 * during that silence loses the whole reply rather than the tail of one.
 *
 * The relay is indifferent to the distinction. It buffers opaque bytes.
 *
 * Needs the fake upstream and an ST with the plugin loaded, same as relay-test.
 */
import http from 'node:http';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const UPSTREAM_PROBE = 'http://127.0.0.1:9911/_probe';
const RELAY = `${ST}/api/plugins/st-relay`;
const CHAT_ID = `nonstream-test-chat-${Date.now()}`;

let cookie = '';
let csrf = '';

const results = [];
function check(name, ok, detail = '') {
    results.push({ name, ok });
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

async function call(url, options = {}) {
    const headers = { ...(options.headers ?? {}) };
    if (cookie) headers['Cookie'] = cookie;
    if (csrf) headers['X-CSRF-Token'] = csrf;

    const response = await fetch(url, { ...options, headers });
    const setCookie = response.headers.getSetCookie?.() ?? [];
    if (setCookie.length > 0) {
        cookie = setCookie.map(c => c.split(';')[0]).join('; ');
    }
    return response;
}

async function probe() {
    return (await fetch(UPSTREAM_PROBE)).json();
}

/** Exactly the relay-test payload, except streaming is off. */
function payload() {
    return {
        api_type: 'ooba',
        api_server: 'http://127.0.0.1:9911',
        stream: false,
        prompt: 'non-streaming test',
        max_tokens: 64,
    };
}

/** Open /stream, then hang up after `ms`, the way a suspended tab would. */
function connectThenVanish(jobId, ms) {
    return new Promise((resolve) => {
        const url = new URL(`${RELAY}/stream`);
        url.searchParams.set('jobId', jobId);
        url.searchParams.set('from', '0');

        const request = http.request(url, {
            headers: { Cookie: cookie, Accept: 'text/event-stream' },
        }, (response) => {
            response.resume();
            setTimeout(() => {
                request.destroy();
                resolve(response.statusCode);
            }, ms);
        });

        request.on('error', () => resolve(null));
        request.end();
    });
}

await (async function handshake() {
    const response = await call(`${ST}/csrf-token`);
    csrf = (await response.json()).token;
    check('obtained CSRF token and session cookie', Boolean(csrf && cookie));
})();

/* ----------------------------------------------------- negative control first */

// Prove the bug exists in this mode before claiming the relay fixes it. Talk to
// ST directly, the way the browser does with the relay disabled, and vanish
// while the provider is still thinking.
{
    const before = await probe();

    await new Promise((resolve) => {
        const url = new URL(`${ST}/api/backends/text-completions/generate`);
        const request = http.request(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Cookie': cookie,
                'X-CSRF-Token': csrf,
            },
        }, (response) => response.resume());

        request.on('error', () => resolve());
        request.end(JSON.stringify(payload()));
        setTimeout(() => { request.destroy(); resolve(); }, 700);
    });

    await new Promise(resolve => setTimeout(resolve, 4000));

    const after = await probe();
    check('control: without the relay, disconnecting kills a non-streaming generation',
        after.aborted === before.aborted + 1,
        `upstream aborts ${before.aborted} -> ${after.aborted}`);
}

/* --------------------------------------------------------- now with the relay */

const before = await probe();

const startResponse = await call(`${RELAY}/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
        target: '/api/backends/text-completions/generate',
        payload: payload(),
        meta: { chatId: CHAT_ID },
    }),
});

if (!startResponse.ok) {
    check('relayed a non-streaming generation', false, `HTTP ${startResponse.status} ${await startResponse.text()}`);
    process.exit(1);
}

const job = await startResponse.json();
check('relayed a non-streaming generation', job.statusCode === 200,
    `job ${job.jobId}, upstream ${job.statusCode}`);

// Vanish while the provider is still thinking. Without the relay this is the
// moment the generation dies, and there is no partial text to fall back on.
await connectThenVanish(job.jobId, 700);
check('client disconnected before the reply was ready', true);

await new Promise(resolve => setTimeout(resolve, 4000));

const after = await probe();
check('the generation survived the disconnect',
    after.aborted === before.aborted && after.completed === before.completed + 1,
    `aborts ${before.aborted} -> ${after.aborted}, completed ${before.completed} -> ${after.completed}`);

const status = await (await call(`${RELAY}/status?jobId=${job.jobId}`)).json();
check('the job finished on the server while nobody was watching',
    status.status === 'done' && status.byteLength > 0,
    `status=${status.status}, ${status.byteLength} bytes`);

const resultResponse = await call(`${RELAY}/result?jobId=${job.jobId}`);
const raw = await resultResponse.text();
let parsed = null;
try {
    parsed = JSON.parse(raw);
} catch {
    // Left null, asserted below.
}

check('the buffered body is intact JSON, not an event stream',
    parsed !== null && !raw.startsWith('data:'),
    `content-type=${resultResponse.headers.get('content-type')}`);

const expected = Array.from({ length: 20 }, (_, i) => `tok${i} `).join('');
check('the complete reply is recoverable after the fact',
    parsed?.choices?.[0]?.text === expected,
    parsed?.choices?.[0]?.text ? `${parsed.choices[0].text.length} chars` : 'no text found');

// A late client replays the whole thing, exactly as in the streaming case.
const replayed = await new Promise((resolve) => {
    const url = new URL(`${RELAY}/stream`);
    url.searchParams.set('jobId', job.jobId);
    url.searchParams.set('from', '0');

    http.request(url, { headers: { Cookie: cookie, Accept: 'text/event-stream' } }, (response) => {
        let buffer = '';
        let text = '';
        response.setEncoding('utf-8');
        response.on('data', (piece) => {
            buffer += piece;
            let boundary = buffer.indexOf('\n\n');
            while (boundary !== -1) {
                const frame = buffer.slice(0, boundary);
                buffer = buffer.slice(boundary + 2);
                boundary = buffer.indexOf('\n\n');
                const type = /^event: (.*)$/m.exec(frame)?.[1];
                const data = /^data: (.*)$/m.exec(frame)?.[1];
                if (type === 'chunk' && data) text += Buffer.from(data, 'base64').toString('utf-8');
                if (type === 'end') resolve(text);
            }
        });
        response.on('end', () => resolve(text));
    }).end();
});

check('a reconnecting client replays the non-streaming body too',
    JSON.parse(replayed || '{}')?.choices?.[0]?.text === expected);

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
