/**
 * Stands in for a text-completion backend. Records whether the caller
 * (SillyTavern) ever aborts, and reports its observations on GET /_probe.
 *
 * Answers in whichever mode the request asks for. With `stream: true` it sends
 * 20 SSE chunks at 300ms intervals. With streaming off it waits, then returns
 * one JSON body: the case where the user has disabled streaming in ST, which
 * takes a different branch in ST's backend and needs covering too.
 */
import http from 'node:http';

const CHUNKS = 20;
const INTERVAL_MS = 300;
const NONSTREAM_DELAY_MS = 3000;

const state = { requests: 0, aborted: 0, completed: 0, sent: 0 };

/** The same text either mode produces, so tests can assert on one string. */
function fullText() {
    return Array.from({ length: CHUNKS }, (_, i) => `tok${i} `).join('');
}

function readBody(req) {
    return new Promise((resolve) => {
        let raw = '';
        req.setEncoding('utf-8');
        req.on('data', piece => { raw += piece; });
        req.on('end', () => {
            try {
                resolve(JSON.parse(raw));
            } catch {
                resolve({});
            }
        });
    });
}

/** One JSON body after a delay, in the shape ST's non-streaming branch reads. */
function serveNonStreaming(res) {
    let closedEarly = false;

    const timer = setTimeout(() => {
        if (closedEarly) {
            return;
        }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ choices: [{ index: 0, text: fullText() }] }));
        state.completed += 1;
        state.sent += CHUNKS;
        console.log('[upstream] completed a non-streaming reply');
    }, NONSTREAM_DELAY_MS);

    res.on('close', () => {
        if (!res.writableEnded) {
            closedEarly = true;
            clearTimeout(timer);
            state.aborted += 1;
            console.log('[upstream] ABORTED by caller before the non-streaming reply was sent');
        }
    });
}

const server = http.createServer(async (req, res) => {
    if (req.url === '/_probe') {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(state));
        return;
    }

    if (req.method !== 'POST') {
        res.statusCode = 404;
        res.end();
        return;
    }

    state.requests += 1;
    let index = 0;
    let closedEarly = false;

    const body = await readBody(req);

    if (body.stream === false) {
        serveNonStreaming(res);
        return;
    }

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    });

    const timer = setInterval(() => {
        if (closedEarly) {
            return;
        }

        if (index >= CHUNKS) {
            clearInterval(timer);
            res.write('data: [DONE]\n\n');
            res.end();
            state.completed += 1;
            console.log(`[upstream] completed after ${CHUNKS} chunks`);
            return;
        }

        const payload = { choices: [{ index: 0, text: `tok${index} ` }] };
        res.write(`data: ${JSON.stringify(payload)}\n\n`);
        state.sent += 1;
        index += 1;
    }, INTERVAL_MS);

    res.on('close', () => {
        if (index < CHUNKS) {
            closedEarly = true;
            clearInterval(timer);
            state.aborted += 1;
            console.log(`[upstream] ABORTED by caller after ${index} chunks`);
        }
    });
});

server.listen(9911, '127.0.0.1', () => console.log('[upstream] listening on 127.0.0.1:9911'));
