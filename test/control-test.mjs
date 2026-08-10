/**
 * Negative control: talk to ST's generate endpoint the way the browser normally
 * does, disconnect mid-stream, and confirm the upstream generation dies. This is
 * the bug the relay exists to fix; if this passes, the relay test above is
 * measuring something real.
 */
import http from 'node:http';

const ST = process.env.ST_BASE ?? 'http://127.0.0.1:7788';

const csrfResponse = await fetch(`${ST}/csrf-token`);
const cookie = (csrfResponse.headers.getSetCookie?.() ?? []).map(c => c.split(';')[0]).join('; ');
const { token } = await csrfResponse.json();

const before = await (await fetch('http://127.0.0.1:9911/_probe')).json();

await new Promise((resolve) => {
    const request = http.request(`${ST}/api/backends/text-completions/generate`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Cookie': cookie,
            'X-CSRF-Token': token,
        },
    }, (response) => {
        let chunks = 0;
        response.on('data', (piece) => {
            chunks += piece.toString().split('\n\n').filter(f => f.startsWith('data:')).length;
            if (chunks >= 3) {
                console.log(`[control] disconnecting after ~${chunks} chunks, as a suspended tab would`);
                request.destroy();
                resolve();
            }
        });
        response.on('end', resolve);
    });

    request.on('error', () => resolve());
    request.end(JSON.stringify({
        api_type: 'ooba',
        api_server: 'http://127.0.0.1:9911',
        stream: true,
        prompt: 'control test',
        max_tokens: 64,
    }));
});

await new Promise(resolve => setTimeout(resolve, 2500));
const after = await (await fetch('http://127.0.0.1:9911/_probe')).json();

const died = after.aborted === before.aborted + 1;
console.log(`${died ? 'PASS' : 'FAIL'}  without the relay, a client disconnect aborts the upstream generation ` +
    `(upstream aborts ${before.aborted} -> ${after.aborted}, only ${after.sent - before.sent} chunks sent of 20)`);

process.exit(died ? 0 : 1);
