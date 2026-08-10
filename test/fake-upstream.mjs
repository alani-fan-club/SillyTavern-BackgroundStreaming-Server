/**
 * Stands in for a text-completion backend. Streams 20 SSE chunks at 300ms
 * intervals and records whether the caller (SillyTavern) ever aborts.
 * Reports its observations on GET /_probe.
 */
import http from 'node:http';

const CHUNKS = 20;
const INTERVAL_MS = 300;

const state = { requests: 0, aborted: 0, completed: 0, sent: 0 };

const server = http.createServer((req, res) => {
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

    req.resume();

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
