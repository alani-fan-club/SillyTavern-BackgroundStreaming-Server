/**
 * End-to-end test for the st-relay plugin.
 *
 * The scenario it reproduces is precisely the reported bug: a client starts a
 * streaming generation, disappears mid-stream, comes back, and expects the whole
 * message. Also asserts the control case — that Stop still cancels upstream.
 */
import http from 'node:http';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';
const UPSTREAM_PROBE = 'http://127.0.0.1:9911/_probe';
const RELAY = `${ST}/api/plugins/st-relay`;

// Finished jobs are retained for 15 minutes, so successive runs would otherwise
// see each other's leftovers in the recovery listing.
const CHAT_ID = `relay-test-chat-${Date.now()}`;

let cookie = '';
let csrf = '';

const results = [];

function check(name, ok, detail = '') {
    results.push({ name, ok, detail });
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

async function handshake() {
    const response = await call(`${ST}/csrf-token`);
    const body = await response.json();
    csrf = body.token;
    check('obtained CSRF token and session cookie', Boolean(csrf && cookie));
}

async function probe() {
    return (await fetch(UPSTREAM_PROBE)).json();
}

function payload() {
    return {
        api_type: 'ooba',
        api_server: 'http://127.0.0.1:9911',
        stream: true,
        prompt: 'relay test',
        max_tokens: 64,
    };
}

/**
 * Read the relay SSE stream, returning after `stopAfterChunks` chunks (leaving
 * the connection to be destroyed) or when the job ends.
 */
function readStream(jobId, from, stopAfterChunks) {
    return new Promise((resolve, reject) => {
        const url = new URL(`${RELAY}/stream`);
        url.searchParams.set('jobId', jobId);
        url.searchParams.set('from', String(from));

        const request = http.request(url, {
            headers: { Cookie: cookie, Accept: 'text/event-stream' },
        }, (response) => {
            if (response.statusCode !== 200) {
                reject(new Error(`stream returned ${response.statusCode}`));
                return;
            }

            let buffer = '';
            let offset = from;
            let text = '';
            let chunks = 0;
            let ended = null;

            response.setEncoding('utf-8');
            response.on('data', (piece) => {
                buffer += piece;
                let boundary = buffer.indexOf('\n\n');

                while (boundary !== -1) {
                    const frame = buffer.slice(0, boundary);
                    buffer = buffer.slice(boundary + 2);
                    boundary = buffer.indexOf('\n\n');

                    const type = /^event: (.*)$/m.exec(frame)?.[1];
                    const id = /^id: (.*)$/m.exec(frame)?.[1];
                    const data = /^data: (.*)$/m.exec(frame)?.[1];
                    if (!type || data === undefined) continue;

                    if (type === 'chunk') {
                        const bytes = Buffer.from(data, 'base64');
                        offset = Number(id) + bytes.length;
                        text += bytes.toString('utf-8');
                        chunks += 1;

                        if (stopAfterChunks && chunks >= stopAfterChunks) {
                            request.destroy();
                            resolve({ offset, text, chunks, ended: null, disconnected: true });
                            return;
                        }
                    } else if (type === 'end') {
                        ended = JSON.parse(data);
                        resolve({ offset, text, chunks, ended, disconnected: false });
                        return;
                    }
                }
            });

            response.on('end', () => resolve({ offset, text, chunks, ended, disconnected: !ended }));
            response.on('error', () => resolve({ offset, text, chunks, ended, disconnected: true }));
        });

        request.on('error', (error) => {
            if (error.code !== 'ECONNRESET') reject(error);
        });
        request.end();
    });
}

function countTokens(text) {
    return [...text.matchAll(/tok(\d+) /g)].map(m => Number(m[1]));
}

async function testHealth() {
    const response = await call(`${RELAY}/health`);
    const ok = response.ok;
    const body = ok ? await response.json() : null;
    check('relay plugin is mounted and healthy', ok && body?.ok === true,
        ok ? `v${body.version}, ${body.allowedTargets.length} allowed targets` : `HTTP ${response.status}`);
}

async function testAllowlist() {
    const response = await call(`${RELAY}/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ target: '/api/settings/get', payload: {} }),
    });
    check('rejects a target outside the allowlist', response.status === 400, `HTTP ${response.status}`);
}

async function testDisconnectAndResume() {
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
        check('started a relayed generation', false, `HTTP ${startResponse.status} ${await startResponse.text()}`);
        return;
    }

    const job = await startResponse.json();
    check('started a relayed generation', job.statusCode === 200, `job ${job.jobId}, upstream ${job.statusCode}`);

    // Act as a client that vanishes after 3 chunks.
    const first = await readStream(job.jobId, 0, 3);
    check('received the beginning of the stream', first.chunks >= 3, `${first.chunks} chunks, ${first.offset} bytes`);

    // Stay away long enough that an unrelayed generation would have been killed.
    await new Promise(resolve => setTimeout(resolve, 3000));

    const during = await probe();
    check('upstream generation survived the client disconnect',
        during.aborted === before.aborted,
        `aborts before=${before.aborted} after=${during.aborted}, upstream sent ${during.sent - before.sent} chunks`);

    // Come back and resume from where we left off.
    const second = await readStream(job.jobId, first.offset, 0);
    check('resumed and ran to completion', second.ended?.status === 'done',
        `status=${second.ended?.status}, ${second.chunks} further chunks`);

    const tokens = countTokens(first.text + second.text);
    const expected = Array.from({ length: 20 }, (_, i) => i);
    const complete = JSON.stringify(tokens) === JSON.stringify(expected);
    check('reassembled message is complete and in order with no duplicates',
        complete, `got ${tokens.length}/20 tokens${complete ? '' : `: ${tokens.join(',')}`}`);

    // A late arrival should still be able to replay the whole thing.
    const replay = await readStream(job.jobId, 0, 0);
    const replayTokens = countTokens(replay.text);
    check('full replay from offset 0 works after completion',
        JSON.stringify(replayTokens) === JSON.stringify(expected),
        `${replayTokens.length}/20 tokens`);
}

async function testAbortStillCancels() {
    const before = await probe();

    const startResponse = await call(`${RELAY}/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            target: '/api/backends/text-completions/generate',
            payload: payload(),
            meta: { chatId: 'relay-test-abort' },
        }),
    });

    const job = await startResponse.json();
    await new Promise(resolve => setTimeout(resolve, 900));

    const abortResponse = await call(`${RELAY}/abort`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jobId: job.jobId }),
    });
    const aborted = await abortResponse.json();
    check('abort marks the job aborted', aborted.status === 'aborted', `status=${aborted.status}`);

    await new Promise(resolve => setTimeout(resolve, 700));
    const after = await probe();
    check('abort propagates all the way to the upstream provider',
        after.aborted === before.aborted + 1,
        `upstream aborts ${before.aborted} -> ${after.aborted}`);
}

async function testJobListing() {
    const response = await call(`${RELAY}/jobs`);
    const { jobs } = await response.json();
    const recoverable = jobs.filter(j => j.meta?.chatId === CHAT_ID && j.status === 'done');
    check('finished jobs are listed for recovery', recoverable.length === 1,
        `${jobs.length} job(s) retained, ${recoverable.length} recoverable for the test chat`);
}

await handshake();
await testHealth();
await testAllowlist();
await testDisconnectAndResume();
await testAbortStillCancels();
await testJobListing();

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
