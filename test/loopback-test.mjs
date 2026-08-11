/**
 * Which address the relay dials for its inner hop.
 *
 * This is not cosmetic. That hop re-enters SillyTavern's whitelist middleware,
 * so the address chosen here decides whether the relay works at all on a given
 * deployment. Dialing the address the outer request arrived on looks harmless
 * and fails on any host reachable at more than one address, because users
 * whitelist where their clients connect from, not where their server answers.
 *
 * Pure function, so no server is needed. Run it directly:
 *   node test/loopback-test.mjs
 */
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { __test } = require('../index.js');

const results = [];
function check(name, ok, detail = '') {
    results.push({ name, ok });
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}

/** Stands in for the express request, which is only read for its socket. */
function requestFrom(localAddress, localFamily) {
    return { socket: { localAddress, localFamily } };
}

function candidates(localAddress, localFamily) {
    return __test.loopbackCandidates(requestFrom(localAddress, localFamily));
}

/* ------------------------------------------------------- the reported break */

// A host reached over a VPN or overlay network answers on an address nobody has
// whitelisted for the host itself. Loopback has to be tried first, or every such
// deployment gets a 403 on the inner hop while working fine over localhost.
{
    const chosen = candidates('100.68.23.76', 'IPv4');
    check('a request arriving on a non-loopback address still dials loopback first',
        chosen[0] === '127.0.0.1', chosen.join(' then '));
    check('the arrival address is kept as a fallback, not discarded',
        chosen.includes('100.68.23.76'), chosen.join(' then '));
}

// Same shape, different deployment: a LAN address, a second NIC, a public IP.
for (const address of ['192.168.1.50', '10.0.0.200', '172.16.4.9', '203.0.113.7']) {
    const chosen = candidates(address, 'IPv4');
    check(`loopback is preferred for an arrival address of ${address}`,
        chosen[0] === '127.0.0.1' && chosen[1] === address, chosen.join(' then '));
}

/* ------------------------------------------------------------ address families */

// ST can be configured IPv6-only, where 127.0.0.1 is not listening at all.
{
    const chosen = candidates('fd7a:115c:a1e0::5c01:174d', 'IPv6');
    check('an IPv6 request dials the IPv6 loopback, not 127.0.0.1',
        chosen[0] === '::1', chosen.join(' then '));
}

// A v4 peer on a dual-stack listener is reported as ::ffff:127.0.0.1. That is a
// v4 connection wearing v6 notation, so it wants the v4 loopback.
{
    const chosen = candidates('::ffff:192.168.1.50', 'IPv6');
    check('a v4-mapped address is treated as IPv4',
        chosen[0] === '127.0.0.1' && chosen[1] === '192.168.1.50', chosen.join(' then '));
}

/* ----------------------------------------------------------------- degenerate */

{
    const chosen = candidates('127.0.0.1', 'IPv4');
    check('an already-loopback request produces no pointless duplicate',
        chosen.length === 1 && chosen[0] === '127.0.0.1', chosen.join(' then '));
}
{
    const chosen = candidates('::1', 'IPv6');
    check('the same holds for IPv6 loopback',
        chosen.length === 1 && chosen[0] === '::1', chosen.join(' then '));
}
{
    const chosen = candidates('::ffff:127.0.0.1', 'IPv6');
    check('a v4-mapped loopback collapses to one candidate',
        chosen.length === 1 && chosen[0] === '127.0.0.1', chosen.join(' then '));
}
{
    // Sockets that report nothing at all, which some exotic setups manage.
    const chosen = candidates(undefined, undefined);
    check('a socket with no local address still yields a usable default',
        chosen.length === 1 && chosen[0] === '127.0.0.1', chosen.join(' then '));
}

/* ------------------------------------------------- middleware-rejection sniff */

const blocked = __test.isBlockedByMiddleware;

check('an HTML 403 is read as a middleware rejection',
    blocked({ statusCode: 403, contentType: 'text/html; charset=utf-8' }) === true);
check('an HTML 401 is read as a middleware rejection',
    blocked({ statusCode: 401, contentType: 'text/html' }) === true);
check("a provider's own JSON 401 is left alone",
    blocked({ statusCode: 401, contentType: 'application/json' }) === false);
check("a provider's own JSON 403 is left alone",
    blocked({ statusCode: 403, contentType: 'application/json' }) === false);
check('a normal event stream is left alone',
    blocked({ statusCode: 200, contentType: 'text/event-stream' }) === false);
check('a 5xx HTML page is not mistaken for a rejection',
    blocked({ statusCode: 502, contentType: 'text/html' }) === false);

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
process.exit(failed.length === 0 ? 0 : 1);
