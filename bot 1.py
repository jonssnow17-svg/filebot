"""
Telegram File -> Download Link bot  (CLOUD v2, for Render)

Required Environment Variables:
    API_ID, API_HASH, BOT_TOKEN

Optional Environment Variables (speed + safety):
    BIN_CHANNEL       id of a private storage channel, e.g. -1001234567890
                      (needed for multi-bot mode; also makes links survive
                      even if you delete your chat with the bot)
    EXTRA_BOT_TOKENS  more bot tokens separated by commas (needs BIN_CHANNEL,
                      every bot must be an ADMIN of that channel)
    ALLOWED_USERS     your Telegram user id(s), comma separated. If set, only
                      these people can use the bot (protects your bandwidth)
    WINDOW            max parallel blocks per download (default 8)
    BLOCK_MB          block size in MB (default 4)

Speed features:
  * Parallel block downloading (many Telegram requests at the same time)
  * Multi-bot mode: blocks are spread across all bots
Links are signed and stateless, so they keep working after restarts.
Max file size = Telegram limit (2 GB, 4 GB if the sender has Premium).
"""
import asyncio
import hashlib
import hmac
import os
import re
from collections import deque
from urllib.parse import quote

from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.types import InputChannel

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", "10000"))
BASE_URL = (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("BASE_URL") or "").rstrip("/")

EXTRA_TOKENS = [t.strip() for t in os.environ.get("EXTRA_BOT_TOKENS", "").split(",") if t.strip()]
_bin = os.environ.get("BIN_CHANNEL", "").strip()
BIN_CHANNEL = int(_bin) if _bin else None
ALLOWED = {int(x) for x in re.findall(r"-?\d+", os.environ.get("ALLOWED_USERS", ""))}

CHUNK = 1024 * 1024  # Telegram max request size (1 MB)
BLOCK = max(1, int(os.environ.get("BLOCK_MB", "4"))) * CHUNK
MAX_WINDOW = max(1, int(os.environ.get("WINDOW", "8")))

CLIENTS = []  # CLIENTS[0] is the main bot


# ------------------------------------------------------------ helpers
def sign(chat, msg):
    return hmac.new(BOT_TOKEN.encode(), f"{chat}:{msg}".encode(), hashlib.sha256).hexdigest()[:24]


def make_token(chat, msg):
    return f"{chat}_{msg}_{sign(chat, msg)}"


def read_token(token):
    try:
        chat, msg, sig = token.split("_")
        if hmac.compare_digest(sig, sign(chat, msg)):
            return int(chat), int(msg)
    except Exception:
        pass
    return None


def human(n):
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def parse_range(header, size):
    m = re.match(r"bytes=(\d*)-(\d*)$", header or "")
    if not m:
        return None
    s, e = m.groups()
    if s == "" and e == "":
        return None
    if s == "":
        start, end = max(size - int(e), 0), size - 1
    else:
        start = int(s)
        end = min(int(e), size - 1) if e else size - 1
    if start > end or start >= size:
        return "bad"
    return start, end


async def prime_channel(cl):
    """Let a bot 'see' the storage channel (bots start with an empty cache)."""
    cid = int(str(BIN_CHANNEL)[4:]) if str(BIN_CHANNEL).startswith("-100") else abs(BIN_CHANNEL)
    await cl(GetChannelsRequest([InputChannel(cid, 0)]))


# ------------------------------------------------------------ downloading
async def get_pool(chat, msg_id):
    """Return [(client, message)] for every bot that can read this file."""
    if BIN_CHANNEL is not None and chat == BIN_CHANNEL:
        clients = list(CLIENTS)
    else:
        clients = CLIENTS[:1]

    async def one(cl):
        try:
            m = await cl.get_messages(chat, ids=msg_id)
            if m and m.file:
                return cl, m
        except Exception as e:
            print("client cannot read file:", repr(e))
        return None

    res = await asyncio.gather(*(one(c) for c in clients))
    return [r for r in res if r]


async def fetch_block(pool, idx, off, expected):
    last = None
    for attempt in range(4):
        cl, msg = pool[(idx + attempt) % len(pool)]
        try:
            buf = bytearray()
            async for chunk in cl.iter_download(
                msg.media, offset=off, limit=BLOCK // CHUNK, request_size=CHUNK
            ):
                buf += chunk
            if len(buf) >= expected:
                return bytes(buf[:expected])
            last = Exception(f"short block {len(buf)}/{expected}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last = e
        await asyncio.sleep(0.5 * (attempt + 1))
    raise last


async def stream_range(resp, pool, start, end, size):
    aligned = start - (start % BLOCK)
    skip = start - aligned
    remaining = end - start + 1
    window = max(2, min(MAX_WINDOW, 3 * len(pool)))
    offsets = iter(range(aligned, end + 1, BLOCK))
    tasks = deque()
    counter = 0

    def launch():
        nonlocal counter
        off = next(offsets, None)
        if off is None:
            return
        expected = min(BLOCK, size - off)
        tasks.append(asyncio.create_task(fetch_block(pool, counter, off, expected)))
        counter += 1

    try:
        for _ in range(window):
            launch()
        while tasks:
            data = await tasks.popleft()
            launch()
            if skip:
                data = data[skip:]
                skip = 0
            if len(data) > remaining:
                data = data[:remaining]
            await resp.write(data)
            remaining -= len(data)
            if remaining <= 0:
                break
    finally:
        for t in tasks:
            t.cancel()


# ------------------------------------------------------------ web
async def health(request):
    return web.Response(text=f"ok, bots ready: {len(CLIENTS)}")


async def download(request):
    if not CLIENTS:
        raise web.HTTPServiceUnavailable(text="Starting up, try again in a few seconds.")
    ids = read_token(request.match_info["token"])
    if not ids:
        raise web.HTTPNotFound(text="Link not found.")
    pool = await get_pool(*ids)
    if not pool:
        raise web.HTTPNotFound(text="File is no longer available.")

    f = pool[0][1].file
    size = f.size
    name = f.name or f"file{f.ext or ''}"
    mime = f.mime_type or "application/octet-stream"

    start, end, status = 0, size - 1, 200
    rng = parse_range(request.headers.get("Range"), size)
    if rng == "bad":
        raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{size}"})
    if rng:
        start, end = rng
        status = 206

    headers = {
        "Content-Type": mime,
        "Content-Length": str(end - start + 1),
        "Accept-Ranges": "bytes",
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    resp = web.StreamResponse(status=status, headers=headers)
    await resp.prepare(request)
    if request.method == "HEAD":
        return resp
    try:
        await stream_range(resp, pool, start, end, size)
    except (ConnectionResetError, ConnectionError):
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print("download failed:", repr(e))
        if request.transport:
            request.transport.close()
    return resp


# ------------------------------------------------------------ telegram
async def start_extras():
    if not EXTRA_TOKENS:
        return
    if BIN_CHANNEL is None:
        print("EXTRA_BOT_TOKENS ignored: set BIN_CHANNEL to use multi-bot mode.")
        return
    for t in EXTRA_TOKENS:
        try:
            c = TelegramClient(StringSession(), API_ID, API_HASH)
            await c.start(bot_token=t)
            await prime_channel(c)
            CLIENTS.append(c)
            me = await c.get_me()
            print("Extra bot ready:", me.username)
        except Exception as e:
            print("Extra bot failed:", repr(e))
        await asyncio.sleep(1)


async def main():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_get("/d/{token}", download)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    print("Web server started on port", PORT)

    c = TelegramClient(StringSession(), API_ID, API_HASH)
    await c.start(bot_token=BOT_TOKEN)
    if BIN_CHANNEL is not None:
        try:
            await prime_channel(c)
        except Exception as e:
            print("Cannot open BIN_CHANNEL. Is the bot an admin there?", repr(e))

    @c.on(events.NewMessage(incoming=True))
    async def on_message(event):
        if ALLOWED and event.sender_id not in ALLOWED:
            await event.reply("Sorry, this bot is private.")
            return
        if not event.file:
            await event.reply(
                "Send me any file (document, video, audio, photo, apk, zip...) "
                "and I will give you a download link for a browser."
            )
            return
        if not BASE_URL:
            await event.reply("Server setting BASE_URL is missing.")
            return
        chat_ref, msg_ref = event.chat_id, event.id
        if BIN_CHANNEL is not None:
            try:
                try:
                    sent = await c.send_file(BIN_CHANNEL, event.message.media)
                except Exception:
                    sent = await c.forward_messages(BIN_CHANNEL, event.message)
                chat_ref, msg_ref = BIN_CHANNEL, sent.id
            except Exception as e:
                print("save to BIN_CHANNEL failed:", repr(e))
                await event.reply(
                    "Could not save the file to the storage channel. "
                    "Make sure the bot is an admin there and can post."
                )
                return
        name = event.file.name or f"file{event.file.ext or ''}"
        await event.reply(
            f"File: {name}\nSize: {human(event.file.size)}\n\n"
            f"Download link:\n{BASE_URL}/d/{make_token(chat_ref, msg_ref)}"
        )

    CLIENTS.append(c)
    print("Bot is running. Multi-bot:", "ON" if EXTRA_TOKENS and BIN_CHANNEL else "off")
    asyncio.create_task(start_extras())
    await c.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
