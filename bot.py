"""
Telegram File -> Download Link bot (CLOUD version for Render)
Settings come from Environment Variables: API_ID, API_HASH, BOT_TOKEN
Links are signed and stateless, so they keep working after restarts.
Streams from Telegram (no disk used). Max file size = Telegram limit (2 GB, 4 GB Premium sender).
"""
import asyncio
import hashlib
import hmac
import os
import re
from urllib.parse import quote

from aiohttp import web
from telethon import TelegramClient, events
from telethon.sessions import StringSession

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", "10000"))
BASE_URL = (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("BASE_URL") or "").rstrip("/")
CHUNK = 512 * 1024  # must divide 1 MB (Telegram rule)

client = None


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


async def health(request):
    return web.Response(text="ok")


async def download(request):
    if client is None:
        raise web.HTTPServiceUnavailable(text="Starting up, try again in a few seconds.")
    ids = read_token(request.match_info["token"])
    if not ids:
        raise web.HTTPNotFound(text="Link not found.")
    try:
        msg = await client.get_messages(ids[0], ids=ids[1])
    except Exception:
        msg = None
    if not msg or not msg.file:
        raise web.HTTPNotFound(text="File is no longer available.")

    size = msg.file.size
    name = msg.file.name or f"file{msg.file.ext or ''}"
    mime = msg.file.mime_type or "application/octet-stream"

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

    aligned = start - (start % CHUNK)
    skip = start - aligned
    remaining = end - start + 1
    try:
        async for chunk in client.iter_download(msg.media, offset=aligned, request_size=CHUNK):
            if skip:
                chunk = chunk[skip:]
                skip = 0
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            await resp.write(chunk)
            remaining -= len(chunk)
            if remaining <= 0:
                break
    except (ConnectionResetError, ConnectionError):
        pass
    return resp


async def main():
    global client
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

    @c.on(events.NewMessage(incoming=True))
    async def on_message(event):
        if not event.file:
            await event.reply(
                "Send me any file (document, video, audio, photo, apk, zip...) "
                "and I will give you a download link for a browser."
            )
            return
        if not BASE_URL:
            await event.reply("Server setting BASE_URL is missing.")
            return
        name = event.file.name or f"file{event.file.ext or ''}"
        token = make_token(event.chat_id, event.id)
        await event.reply(
            f"File: {name}\nSize: {human(event.file.size)}\n\n"
            f"Download link:\n{BASE_URL}/d/{token}"
        )

    client = c
    print("Bot is running.")
    await c.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
