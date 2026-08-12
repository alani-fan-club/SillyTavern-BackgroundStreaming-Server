# SillyTavern Background Streaming (server plugin)

Keeps streaming generations running when the browser tab goes away.

If SillyTavern runs on your desktop and you use it from a phone, you have probably
lost a reply this way: you send a message, lock the screen for ten seconds, come
back, and the response is gone. Not truncated, gone, and you still paid for the
tokens. iOS Safari suspends the backgrounded tab, the socket dies, and SillyTavern
cancels the generation at the provider. Sleeping laptops and dropped Wi-Fi do the
same thing.

This plugin is half of a pair. On its own it does nothing.

| Piece | Repository | Installs into |
|---|---|---|
| Server plugin (this repo) | [SillyTavern-BackgroundStreaming-Server](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server) | `plugins/` |
| UI extension | [SillyTavern-BackgroundStreaming](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming) | `data/<user>/extensions/` |

Both install outside SillyTavern's own tracked files, so running `git pull` on
SillyTavern never conflicts with them and you do not need a fork.

## Installation

> Server plugins are not sandboxed. They run with the full privileges of the
> SillyTavern process and can read your entire filesystem. That applies to every
> ST server plugin, this one included. Read `index.js` before installing it. It is
> a single file with no dependencies, which is deliberate: you can actually audit
> it in a sitting.

### 1. Turn on server plugins

In `config.yaml` in your SillyTavern folder:

```yaml
enableServerPlugins: true
```

### 2. Install the plugin

Pick whichever you prefer. Both put the plugin in the same place.

**From a release zip.** Download the zip from the
[latest release](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server/releases/latest)
and unpack it into your SillyTavern `plugins/` folder. You should end up with
`plugins/SillyTavern-BackgroundStreaming-Server/index.js`. Nothing else to do:
there is no build step and no `npm install`, because the plugin uses only Node
built-ins.

**With git**, which also gets you automatic updates:

```bash
cd plugins
git clone https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server
```

**With SillyTavern's own installer**, run from the SillyTavern folder:

```bash
node plugins.js install https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming-Server
```

### 3. Restart SillyTavern

Watch the console for:

```
[st-relay] ready — detached generation relay mounted at /api/plugins/st-relay
```

If that line is missing, either `enableServerPlugins` is still `false` or the
files landed somewhere other than `plugins/`.

### 4. Install the UI extension

Follow the instructions in
[SillyTavern-BackgroundStreaming](https://github.com/alani-fan-club/SillyTavern-BackgroundStreaming).
Its settings panel tells you whether it can reach this plugin, which is the
fastest way to confirm both halves are talking to each other.

## Updating

SillyTavern runs `git pull` on every git repository in `plugins/` at startup, so a
git install stays current on its own. To turn that off, set
`enableServerPluginsAutoUpdate: false` in `config.yaml` and update by hand:

```bash
git -C plugins/SillyTavern-BackgroundStreaming-Server pull
```

A zip install has no git metadata, so SillyTavern skips it. Replace the folder
with a newer zip when you want to update.

## Uninstalling

Delete the folder and restart:

```bash
rm -rf plugins/SillyTavern-BackgroundStreaming-Server
```

The extension notices the relay is gone and quietly falls back to normal direct
generations, so removing this half by itself is safe.

## Why it is needed

Normally the browser holds the SSE connection to `/api/backends/.../generate`
itself, and every backend registers this:

```js
request.socket.on('close', () => controller.abort());
```

When the tab is suspended the socket dies, that handler fires, and the request to
the provider is aborted. Nothing was buffered anywhere, because the partial text
only ever lived in the client's `StreamingProcessor`. This is upstream issue
[#4007](https://github.com/SillyTavern/SillyTavern/issues/4007).

## How it works

The plugin puts a buffering hop in the middle. It re-issues the same request over
loopback to SillyTavern's own endpoint, passing the caller's session cookie and
CSRF token through untouched. The socket that ST's endpoint sees now belongs to
the plugin, which never goes away, so nothing gets aborted. Response bytes are
buffered and replayed to as many successive client connections as you like,
resumable by byte offset.

That inner hop always goes to loopback, never to whatever address your request
arrived on, because it passes through ST's whitelist a second time. Users
whitelist the addresses their clients connect from, not the ones their server
answers on, so dialing the arrival address works over localhost and the LAN and
then fails on a VPN or overlay network. Loopback is the one address ST puts in
the whitelist itself. If it is ever refused anyway, the plugin retries via the
arrival address before giving up.

```
browser  ──(dies and reconnects freely)──►  relay  ──(never dies)──►  ST endpoint  ──►  provider
```

The plugin knows nothing about any LLM provider. The payload and the response body
are opaque bytes to it, and the extension's only integration point is
`globalThis.fetch`. That is why the pair keeps working across SillyTavern updates
instead of breaking every time an API shape changes.

It is also why turning streaming off in SillyTavern changes nothing here. The bug
is not about streaming: ST registers its abort-on-disconnect handler before it
branches on `stream`, so a non-streaming generation dies on a dropped socket just
the same, and loses the entire reply rather than the tail of one. The relay
buffers a single JSON body exactly as it buffers an event stream.

### Telling "Stop" apart from "suspended"

The one distinction the relay has to get right is the user pressing Stop versus
the tab going away, because both look like a closed socket from the server side.
SillyTavern passes its own `AbortSignal` into `fetch` and fires it on Stop. A
suspended tab never fires that signal. So cancellation is driven only by that
signal, and a dropped connection is always treated as something to resume.

## API

Every route sits under `/api/plugins/st-relay` and inherits SillyTavern's normal
auth, whitelist, and CSRF protection, because plugin routers mount after that
middleware. Jobs belong to the user who created them and are not visible to
anyone else.

| Route | Purpose |
|---|---|
| `POST /start` | `{ target, payload, meta }` returns job info. Resolves once upstream headers arrive, so the real status code reaches the browser. |
| `GET /stream?jobId&from=N` | Replay from byte offset `N`, then follow live. Reconnect as often as you want; dropping this never affects the job. |
| `POST /abort` | `{ jobId }`. The only thing that cancels a generation. |
| `GET /jobs` | Your retained jobs, for recovering one after a page reload. |
| `GET /status?jobId` | One job's state. |
| `GET /result?jobId` | The full buffered body. |
| `POST /consume` | `{ jobId }`. Marks a job as dealt with. |
| `GET /health` | Version, your job count, allowed targets. |

`/stream` sends `event: chunk` frames with a base64 payload and `id` set to the
absolute byte offset, then a final `event: end`. Base64 rather than raw bytes
keeps resume offsets exact and lets you debug the stream with `curl`.

Nothing in `/stream` requires the caller to be the page that started the job,
which is what makes recovery from a destroyed page possible: a freshly loaded
page lists `/jobs`, finds one still running, and streams it from offset zero as
if it had been there all along. The relay does not distinguish the two cases.

### Limits

`MAX_JOB_BYTES` 16 MB, `MAX_JOBS` 64 retained, `MAX_LIVE_JOBS_PER_USER` 8 running
at once, `RETENTION_MS` 60 minutes for finished jobs,
`UPSTREAM_HEADERS_TIMEOUT_MS` 60 seconds. They are plain constants at the top of
`index.js` if you want different numbers.

Nothing limits how long a generation itself may run. The 60 second timeout covers
only the headers phase of the loopback call, so a model that thinks for an hour
before its first token is fine. `RETENTION_MS` is the one that decides how long
you can wander off: it is the window in which a finished reply is still there
waiting when you come back.

Past those limits `/start` returns 503, and the extension reads that as "relay
unavailable" and sends the generation directly instead. You lose background
resumption for that one message rather than the message itself.

## Adding a backend

`ALLOWED_TARGETS` in `index.js` is a strict allowlist. Without it this route would
be an authenticated SSRF proxy into arbitrary ST internals. To relay a new
endpoint, add its path there and to `RELAY_TARGETS` in the extension's `index.js`.
The two lists have to match.

## Tests

Nothing to install beyond Node. `test/fake-upstream.mjs` stands in for a
text-completion backend and reports whether it was ever aborted, which is how the
interesting assertions get made.

```bash
node test/fake-upstream.mjs &                  # fake provider on :9911
node ../../server.js --port 7788 &             # ST with the plugin loaded

node test/loopback-test.mjs                    # inner-hop address choice, no server needed
node test/control-test.mjs                     # negative control: reproduces the bug
node test/relay-test.mjs                       # server plugin, end to end
node test/shim-test.mjs                        # UI extension, end to end
node test/nonstreaming-test.mjs                # the same, with streaming turned off
node test/reload-test.mjs                      # page destroyed mid-generation, then reloaded
node test/duplicate-test.mjs                   # a generation the page received is never recovered too
node test/self-adopt-test.mjs                  # a page never adopts a job it started itself
node test/start-failure-test.mjs               # the socket dies while /start is in flight
```

`fake-upstream.mjs` takes `POST /_config {"headerDelayMs":N}`, which holds the
response headers back. That is the window in which a job is already registered
here but `/start` has not yet told the caller its id, and it is as wide as the
provider is slow — a reasoning model can sit there for tens of seconds.

Point `ST_BASE` at a non-loopback address of the same machine to exercise the
case that matters for remote access, where the request arrives on one address and
the inner hop has to go somewhere else.

| Variable | Default |
|---|---|
| `ST_BASE` | `http://127.0.0.1:7788` |
| `ST_USER` | `default-user` |
| `ST_EXTENSION_DIR` | `<ST>/data/<ST_USER>/extensions/SillyTavern-BackgroundStreaming` |

`control-test` is the one that keeps the others honest. It talks to ST directly
the way the browser normally does, disconnects mid-stream, and asserts that the
upstream generation dies. If it ever starts failing, upstream has fixed the bug
and you may not need this plugin any more.

## Known limits

Buffered jobs live in memory, so restarting SillyTavern loses them.

Recovery text extraction is best effort and lives in the extension. When it cannot
decode a response, the raw buffer is still sitting at `GET /result`.

## License

AGPL-3.0-or-later, matching SillyTavern.
