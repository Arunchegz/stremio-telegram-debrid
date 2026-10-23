import time
import logging
import asyncio
import functools
import inspect
import re
from hashlib import sha256
from typing import Callable, Optional, AsyncGenerator, Union

from pyrogram import Client, raw, utils
from pyrogram.types import Message
from pyrogram.session.auth import Auth
from pyrogram.session import Session
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram import utils as pyro_utils
from pyrogram.errors import VolumeLocNotFound, CDNFileHashMismatch, FloodWait
from pyrogram.crypto import aes
import pyrogram
from config import Config
from utils import parse_split_info
import cache

logger = logging.getLogger("tg_client")

CHUNK_SIZE = 1024 * 1024  # Telegram's upload.getFile maximum


class TelegramError(RuntimeError):
    """Base error for problems that should be reported as a stream failure."""


class TelegramUnavailable(TelegramError):
    """No usable Telegram client is currently connected."""


class StreamingError(TelegramError):
    """A media read failed after the configured retries."""


class BotFileLimitError(TelegramError):
    """The file is larger than a bot account can download."""

    def __init__(self, file_size: int, limit_mb: int):
        self.file_size = file_size
        self.limit_mb = limit_mb
        super().__init__(
            f"This file is {file_size / (1024 * 1024):.1f} MB, but bot-token mode "
            f"can only stream files up to {limit_mb} MB. Add USER_SESSION_STRING "
            "(or USER_SESSION_STRINGS) to stream large files."
        )


def _is_file_reference_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    return "filereference" in name or "file_reference" in text or "file reference" in text


def _is_access_error(exc: Exception) -> bool:
    """The account cannot see this chat/message (e.g. a bot that is not an admin)."""
    blob = f"{type(exc).__name__} {exc}".lower()
    return any(word in blob for word in (
        "peeridinvalid", "peer_id_invalid", "channelinvalid", "channel_invalid",
        "channelprivate", "channel_private", "chatadminrequired", "chat_admin_required",
        "usernotparticipant", "user_not_participant", "chatforbidden", "channelforbidden",
        "not available to this telegram session", "bot_method_invalid", "botmethodinvalid",
        "authkeyunregistered", "auth_key_unregistered", "authkeyduplicated", "auth_key_duplicated",
        "sessionrevoked", "session_revoked", "userdeactivated", "user_deactivated",
    ))


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, ConnectionError, OSError)):
        return True
    blob = f"{type(exc).__name__} {exc}".lower()
    return any(word in blob for word in (
        "timeout", "timed out", "connection", "network", "internal server",
        "service unavailable", "server error", "temporarily", "rpc_call_fail",
    ))


def _flood_wait_seconds(exc) -> int:
    value = getattr(exc, "value", None)
    if value is None:
        value = getattr(exc, "x", None)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 5


# Monkey-patch to cache auth keys across media sessions *per account*.
# Auth itself does not keep a client reference, so remember it at construction.
# With multiple session strings a global dc_id-keyed cache would wrongly share
# one account's media auth key with another account.
_original_auth_init = Auth.__init__
_original_auth_create = Auth.create
_auth_key_cache = {}


def _patched_auth_init(self, *args, **kwargs):
    _original_auth_init(self, *args, **kwargs)
    self._rea_client = args[0] if args else kwargs.get("client")


async def _patched_auth_create(self):
    client = getattr(self, "_rea_client", None)
    key = (id(client), self.dc_id)
    if key in _auth_key_cache:
        logger.info(f"Reusing cached auth key for DC{self.dc_id}")
        return _auth_key_cache[key]

    logger.info(f"Generating new auth key for DC{self.dc_id}...")
    auth_key = await _original_auth_create(self)
    _auth_key_cache[key] = auth_key
    return auth_key


Auth.__init__ = _patched_auth_init
Auth.create = _patched_auth_create


def _drop_auth_cache(client):
    """Forget a stopped client's media-DC auth keys (id() could be reused)."""
    client_id = id(client)
    for key in [k for k in _auth_key_cache if k[0] == client_id]:
        _auth_key_cache.pop(key, None)


# Monkey-patch Client.get_file to reuse media sessions and avoid connection overhead
async def _patched_get_file(
    self: Client,
    file_id: FileId,
    file_size: int = 0,
    limit: int = 0,
    offset: int = 0,
    progress: Callable = None,
    progress_args: tuple = ()
) -> Optional[AsyncGenerator[bytes, None]]:
    async with self.get_file_semaphore:
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(
                        chat_id=-file_id.chat_id
                    )
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                photo_id=file_id.media_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

        current = 0
        total = abs(limit) or (1 << 31) - 1
        chunk_size = 1024 * 1024
        offset_bytes = abs(offset) * chunk_size

        dc_id = file_id.dc_id

        async with self.media_sessions_lock:
            session = self.media_sessions.get(dc_id)
            if session is None:
                logger.info(f"Creating new media session for DC{dc_id}...")
                session = Session(
                    self, dc_id,
                    await Auth(self, dc_id, await self.storage.test_mode()).create()
                    if dc_id != await self.storage.dc_id()
                    else await self.storage.auth_key(),
                    await self.storage.test_mode(),
                    is_media=True
                )
                await session.start()

                if dc_id != await self.storage.dc_id():
                    exported_auth = await self.invoke(
                        raw.functions.auth.ExportAuthorization(
                            dc_id=dc_id
                        )
                    )

                    await session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id,
                            bytes=exported_auth.bytes
                        )
                    )
                self.media_sessions[dc_id] = session
            else:
                logger.info(f"Reusing cached media session for DC{dc_id}")

        try:
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=offset_bytes,
                    limit=chunk_size
                ),
                sleep_threshold=30
            )

            if isinstance(r, raw.types.upload.File):
                while True:
                    chunk = r.bytes

                    yield chunk

                    current += 1
                    offset_bytes += chunk_size

                    if progress:
                        func = functools.partial(
                            progress,
                            min(offset_bytes, file_size)
                            if file_size != 0
                            else offset_bytes,
                            file_size,
                            *progress_args
                        )

                        if inspect.iscoroutinefunction(progress):
                            await func()
                        else:
                            await self.loop.run_in_executor(self.executor, func)

                    if len(chunk) < chunk_size or current >= total:
                        break

                    r = await session.invoke(
                        raw.functions.upload.GetFile(
                            location=location,
                            offset=offset_bytes,
                            limit=chunk_size
                        ),
                        sleep_threshold=30
                    )

            elif isinstance(r, raw.types.upload.FileCdnRedirect):
                cdn_session = Session(
                    self, r.dc_id, await Auth(self, r.dc_id, await self.storage.test_mode()).create(),
                    await self.storage.test_mode(), is_media=True, is_cdn=True
                )

                try:
                    await cdn_session.start()

                    while True:
                        r2 = await cdn_session.invoke(
                            raw.functions.upload.GetCdnFile(
                                file_token=r.file_token,
                                offset=offset_bytes,
                                limit=chunk_size
                            )
                        )

                        if isinstance(r2, raw.types.upload.CdnFileReuploadNeeded):
                            try:
                                await session.invoke(
                                    raw.functions.upload.ReuploadCdnFile(
                                        file_token=r.file_token,
                                        request_token=r2.request_token
                                    )
                                )
                            except VolumeLocNotFound:
                                break
                            else:
                                continue

                        chunk = r2.bytes

                        decrypted_chunk = aes.ctr256_decrypt(
                            chunk,
                            r.encryption_key,
                            bytearray(
                                r.encryption_iv[:-4]
                                + (offset_bytes // 16).to_bytes(4, "big")
                            )
                        )

                        hashes = await session.invoke(
                            raw.functions.upload.GetCdnFileHashes(
                                file_token=r.file_token,
                                offset=offset_bytes
                            )
                        )

                        for i, h in enumerate(hashes):
                            cdn_chunk = decrypted_chunk[h.limit * i: h.limit * (i + 1)]
                            CDNFileHashMismatch.check(
                                h.hash == sha256(cdn_chunk).digest(),
                                "h.hash == sha256(cdn_chunk).digest()"
                            )

                        yield decrypted_chunk

                        current += 1
                        offset_bytes += chunk_size

                        if progress:
                            func = functools.partial(
                                progress,
                                min(offset_bytes, file_size) if file_size != 0 else offset_bytes,
                                file_size,
                                *progress_args
                            )

                            if inspect.iscoroutinefunction(progress):
                                await func()
                            else:
                                await self.loop.run_in_executor(self.executor, func)

                        if len(chunk) < chunk_size or current >= total:
                            break
                finally:
                    await cdn_session.stop()
        except Exception as e:
            if isinstance(e, (pyrogram.StopTransmission, asyncio.CancelledError, FloodWait)):
                # FloodWait is transient; the media session is still healthy.
                raise e
            if not _is_file_reference_error(e):
                logger.warning(f"Error in media session for DC{dc_id}, discarding from cache: {e}")
                async with self.media_sessions_lock:
                    if self.media_sessions.get(dc_id) is session:
                        self.media_sessions.pop(dc_id, None)
                try:
                    await session.stop()
                except Exception:
                    pass
            raise e

Client.get_file = _patched_get_file


class _SessionSlot:
    """One Pyrogram client plus its readiness state and live-message cache."""

    def __init__(self, client: Client, label: str, is_bot: bool = False):
        self.client = client
        self.label = label
        self.is_bot = is_bot
        self.ready = False
        # Chats this account verified at startup (bots only stream chats they
        # can actually read). ``chats_checked`` stays False until then.
        self.chats_ok = set()
        self.chats_checked = False


class TelegramClientManager:
    def __init__(self):
        self.user_client = None          # first user client (backwards compatible)
        self.user_clients = []           # all configured user clients
        self.bot_client = None           # first bot client (backwards compatible)
        self.bot_clients = []            # all configured bot clients
        self.client = None               # active primary client (backwards compatible)
        self.is_running = False
        self._user_slots = []
        self._bot_slots = []
        self._user_ready = False
        self._bot_ready = False
        self._bot_only_warned = False

        self._search_cache = {}          # small L1; authoritative cache lives in SQLite
        self._message_cache = {}         # primary-client live messages
        self._client_message_cache = {}  # (client_id, chat_id, message_id) -> (ts, Message)
        self._client_message_locks = {}
        self._client_flood_until = {}
        self._client_bad_until = {}      # id(client) -> ts; skip accounts that cannot read a chat
        self._stream_cursor = 0
        self._log_cache = {}
        self._refresh_locks = {}
        self._refresh_ts = {}
        self._tg_semaphore = asyncio.Semaphore(Config.TG_SEARCH_CONCURRENCY)
        self._tg_spacing_lock = asyncio.Lock()
        self._last_tg_call = 0.0
        self._flood_wait_until = 0.0

    # ------------------------------------------------------------------
    # Capability helpers
    # ------------------------------------------------------------------
    @property
    def has_user_session(self) -> bool:
        return self._user_ready and any(slot.ready for slot in self._user_slots)

    @property
    def has_bot_token(self) -> bool:
        return self._bot_ready and any(slot.ready for slot in self._bot_slots)

    @property
    def is_bot_only(self) -> bool:
        return self.has_bot_token and not self.has_user_session

    def _ready_user_slots(self) -> list:
        return [slot for slot in self._user_slots if slot.ready]

    def _ready_bot_slots(self) -> list:
        return [slot for slot in self._bot_slots if slot.ready]

    def _active_client(self) -> Client:
        """First ready user session (search/logs), bot token as fallback."""
        for slot in self._ready_user_slots():
            return slot.client
        for slot in self._ready_bot_slots():
            return slot.client
        raise TelegramUnavailable("Telegram client is not connected yet")

    def _all_ready_slots(self) -> list:
        return self._ready_user_slots() + self._ready_bot_slots()

    def _client_label(self, client: Client) -> str:
        for slot in self._all_ready_slots():
            if slot.client is client:
                return slot.label
        return "telegram session"

    def _bot_can_stream_chat(self, slot, chat_id) -> bool:
        """A bot may only carry chunks for chats it verified (admin access)."""
        if not slot.chats_checked or chat_id is None:
            return True
        try:
            return int(chat_id) in slot.chats_ok
        except (TypeError, ValueError):
            return chat_id in slot.chats_ok

    def _stream_clients(self, file_size: int = 0, chat_id=None) -> list:
        """Return the clients that may carry stream chunks.

        Every ready user session takes part in the chunk rotation. Bot clients
        join it too (MTProto bots can read large files) unless
        ``TG_BOT_STREAMING`` is off, in which case they are only a fallback
        when no user session is available. A bot that is not a member/admin of
        the chat being streamed is left out.
        """
        self.ensure_streamable(file_size)
        users = [slot.client for slot in self._ready_user_slots()]
        bots = [
            slot.client for slot in self._ready_bot_slots()
            if self._bot_can_stream_chat(slot, chat_id)
        ]
        if users:
            if Config.TG_BOT_STREAMING and bots:
                return users + bots
            return users
        if bots:
            return bots
        raise TelegramUnavailable("Telegram client is not connected yet")

    def ensure_streamable(self, file_size: int = 0):
        """Raise a clear error before headers are sent if the file cannot stream."""
        if self.has_user_session:
            return
        if not self.has_bot_token:
            raise TelegramUnavailable("Telegram client is not connected yet")
        # Optional guard (off by default) for bot-only setups.
        limit_mb = Config.BOT_FILE_LIMIT_MB
        if limit_mb > 0 and file_size and file_size > limit_mb * CHUNK_SIZE:
            raise BotFileLimitError(file_size, limit_mb)

    def _next_stream_offset(self, count: int) -> int:
        """Round-robin start offset so concurrent streams use different sessions."""
        if count <= 1:
            return 0
        offset = self._stream_cursor % count
        self._stream_cursor = (self._stream_cursor + 1) % max(1, count * 1000)
        return offset

    def client_for_stream(self, file_size: int = 0) -> Client:
        """Primary client for whole-file operations (subtitles, small downloads).

        ``download_media`` consumes the message's existing file reference, which
        belongs to the primary session, so this must not rotate to another
        account. Chunk-level rotation happens inside ``iter_media_range`` where
        each account fetches its own live message.
        """
        self.ensure_streamable(file_size)
        return self._active_client()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @staticmethod
    def _new_client(**kwargs) -> Client:
        # max_concurrent_transmissions enables the parallel chunk prefetch.
        # Older Pyrogram builds without the argument still work sequentially.
        try:
            return Client(max_concurrent_transmissions=Config.TG_STREAM_PREFETCH, **kwargs)
        except TypeError:
            return Client(**kwargs)

    def initialize(self):
        Config.validate()

        mode = Config.TG_AUTH_MODE
        sessions = Config.session_strings() if mode in ("auto", "user") else []
        bot_tokens = Config.bot_tokens() if mode in ("auto", "bot") else []

        self.user_client = None
        self.user_clients = []
        self.bot_client = None
        self.bot_clients = []
        self._user_slots = []
        self._bot_slots = []
        self._user_ready = False
        self._bot_ready = False
        self._bot_only_warned = False
        self._client_message_cache.clear()
        self._client_message_locks.clear()
        self._client_flood_until.clear()
        self._client_bad_until.clear()
        self._stream_cursor = 0

        for index, session_string in enumerate(sessions, 1):
            logger.info(
                f"Initializing Telegram user client {index}/{len(sessions)} (session string)..."
            )
            client = self._new_client(
                name=f"tg_stremio_user_{index}",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
            )
            self.user_clients.append(client)
            self._user_slots.append(
                _SessionSlot(client, f"user session {index}" if len(sessions) > 1 else "user session")
            )

        for index, token in enumerate(bot_tokens, 1):
            logger.info(
                f"Initializing Telegram bot client {index}/{len(bot_tokens)} (bot token)..."
            )
            client = self._new_client(
                name=f"tg_stremio_bot_{index}",
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=token,
                in_memory=True,
                no_updates=True,
            )
            self.bot_clients.append(client)
            self._bot_slots.append(
                _SessionSlot(
                    client,
                    f"bot {index}" if len(bot_tokens) > 1 else "bot token",
                    is_bot=True,
                )
            )

        if not self._user_slots and not self._bot_slots:
            raise ValueError(
                "Neither USER_SESSION_STRING nor BOT_TOKEN is usable "
                f"(TG_AUTH_MODE={Config.TG_AUTH_MODE})."
            )

        self.user_client = self.user_clients[0] if self.user_clients else None
        self.bot_client = self.bot_clients[0] if self.bot_clients else None
        self.client = self.user_client or self.bot_client
        self._tg_semaphore = asyncio.Semaphore(Config.TG_SEARCH_CONCURRENCY)

    async def _start_one(self, slot: _SessionSlot) -> bool:
        client = slot.client
        try:
            await asyncio.wait_for(client.start(), timeout=60)
            me = getattr(client, "me", None)
            who = getattr(me, "username", None) or getattr(me, "first_name", None) or "connected"
            logger.info(f"Telegram {slot.label} connected as {who}")
            slot.ready = True
            return True
        except Exception as e:
            logger.error(f"Telegram {slot.label} failed to start: {e}")
            slot.ready = False
            try:
                await asyncio.wait_for(client.stop(), timeout=3.0)
            except Exception:
                pass
            _drop_auth_cache(client)
            return False

    async def start(self):
        if not self.client and not self._user_slots and not self._bot_slots:
            self.initialize()

        if not self.is_running:
            logger.info("Starting Pyrogram client(s)...")
            slots = list(self._user_slots) + list(self._bot_slots)
            results = await asyncio.gather(
                *(self._start_one(slot) for slot in slots), return_exceptions=True
            )
            for slot, result in zip(slots, results):
                if isinstance(result, Exception):
                    logger.error(f"Telegram {slot.label} start error: {result}")
                    slot.ready = False

            self._user_ready = any(slot.ready for slot in self._user_slots)
            self._bot_ready = any(slot.ready for slot in self._bot_slots)
            ready_users = self._ready_user_slots()
            ready_bots = self._ready_bot_slots()

            if ready_users:
                self.user_client = ready_users[0].client
                self.client = self.user_client
            elif ready_bots:
                self.bot_client = ready_bots[0].client
                self.client = self.bot_client
            else:
                self.is_running = False
                if Config.TG_AUTH_MODE == "user":
                    logger.error("TG_AUTH_MODE=user but no session string could connect.")
                raise TelegramUnavailable(
                    "Could not connect to Telegram. Check the session string(s) / bot token "
                    "in /configure (and that the bot is an admin of your channels)."
                )

            self.is_running = True
            connected = len(ready_users)
            if len(self._user_slots) > 1:
                logger.info(
                    f"{connected}/{len(self._user_slots)} user sessions connected; "
                    "stream chunks will rotate across them."
                )
            if self._bot_slots:
                logger.info(
                    f"{len(ready_bots)}/{len(self._bot_slots)} bot clients connected"
                    + (
                        "; they join the stream rotation (TG_BOT_STREAMING=true)."
                        if (ready_users and Config.TG_BOT_STREAMING)
                        else "."
                    )
                )
            if self.is_bot_only:
                limit_note = (
                    f" and files larger than {Config.BOT_FILE_LIMIT_MB} MB are refused"
                    if Config.BOT_FILE_LIMIT_MB > 0 else ""
                )
                logger.warning(
                    "Running with BOT TOKEN(s) only: channel history search is "
                    f"unavailable to bots{limit_note}. "
                    "Add USER_SESSION_STRING for full functionality."
                )

            # Resolve target channels on startup to avoid PeerIdInvalid errors
            await self._resolve_channels()

    async def _seed_bot_peer(self, bot_client: Client, seed_client: Client, chat_id) -> bool:
        """Teach a bot a channel's access hash using a user session.

        A fresh in-memory bot session has no peer cache and (with
        ``no_updates``) never receives updates, so Pyrogram cannot resolve a
        private channel id on its own and raises ``Peer id invalid``. The
        channel's access hash is read from a user session and stored in the
        bot's peer cache; the following ``get_chat`` then proves whether the
        bot really is a participant (otherwise Telegram rejects it).
        """
        peer = await seed_client.resolve_peer(chat_id)
        channel_id = getattr(peer, "channel_id", None)
        access_hash = getattr(peer, "access_hash", None)
        if channel_id is None or access_hash is None:
            return False
        peer_id = pyro_utils.get_channel_id(channel_id)
        await bot_client.storage.update_peers([(peer_id, access_hash, "channel", None, None)])
        await bot_client.get_chat(chat_id)
        return True

    async def _resolve_channels_for_client(self, slot: _SessionSlot, chat_ids: list,
                                           seed_client: Client = None):
        client = slot.client
        try:
            if not slot.is_bot:
                # Walk the dialog list so private channel access hashes are
                # cached in *this* account's peer storage.
                cached_count = 0
                async for dialog in client.get_dialogs(limit=400):
                    if dialog.chat.id in chat_ids:
                        logger.info(
                            f"[{slot.label}] Resolved channel: "
                            f"{dialog.chat.title} ({dialog.chat.id})"
                        )
                        cached_count += 1
                        if cached_count >= len(chat_ids):
                            break

            for chat_id in chat_ids:
                try:
                    await client.get_chat(chat_id)
                    try:
                        slot.chats_ok.add(int(chat_id))
                    except (TypeError, ValueError):
                        slot.chats_ok.add(chat_id)
                except Exception as e:
                    if slot.is_bot and seed_client is not None:
                        try:
                            if await self._seed_bot_peer(client, seed_client, chat_id):
                                try:
                                    slot.chats_ok.add(int(chat_id))
                                except (TypeError, ValueError):
                                    slot.chats_ok.add(chat_id)
                                logger.info(
                                    f"[{slot.label}] Resolved channel {chat_id} "
                                    "(peer seeded from the user session)"
                                )
                                continue
                        except Exception as seed_err:
                            e = f"{e}; after seeding the peer: {seed_err}"
                    hint = (
                        " (the bot must be an admin of this exact channel, and the BOT_TOKEN "
                        "must belong to that bot)" if slot.is_bot else ""
                    )
                    logger.warning(f"[{slot.label}] Failed to cache channel {chat_id}: {e}{hint}")
            slot.chats_checked = True
            if slot.is_bot and chat_ids and not slot.chats_ok:
                logger.warning(
                    f"[{slot.label}] cannot access any configured channel; "
                    "it will not be used for streaming. Add the bot as an admin."
                )

            if Config.LOG_CHANNEL_ID:
                try:
                    await client.get_chat(Config.LOG_CHANNEL_ID)
                except Exception as e:
                    logger.warning(
                        f"[{slot.label}] Failed to cache log channel {Config.LOG_CHANNEL_ID}: {e}"
                    )
        except Exception as e:
            logger.warning(f"[{slot.label}] Failed to resolve target channels: {e}")

    async def _resolve_channels(self):
        try:
            chat_ids = self.get_channel_ids()
            user_slots = self._ready_user_slots()
            bot_slots = self._ready_bot_slots()
            if not user_slots and not bot_slots:
                return
            # User sessions first: they can see the channel and hand its access
            # hash to the bots, which cannot resolve private channels on their own.
            await asyncio.gather(
                *(self._resolve_channels_for_client(slot, chat_ids) for slot in user_slots),
                return_exceptions=True,
            )
            seed_client = user_slots[0].client if user_slots else None
            await asyncio.gather(
                *(self._resolve_channels_for_client(slot, chat_ids, seed_client=seed_client)
                  for slot in bot_slots),
                return_exceptions=True,
            )
        except Exception as e:
            logger.warning(f"Failed to resolve target channels on startup: {e}")

    async def stop(self):
        if (not self.is_running and not self._user_slots and not self._bot_slots):
            return
        logger.info("Stopping Pyrogram client(s)...")
        clients = [slot.client for slot in self._user_slots + self._bot_slots]
        for client in clients:
            try:
                await asyncio.wait_for(client.stop(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning("Pyrogram client stop timed out, skipping...")
            except Exception as e:
                logger.warning(f"Error stopping Pyrogram client: {e}")
            finally:
                _drop_auth_cache(client)
        self.is_running = False
        self._user_ready = False
        self._bot_ready = False
        for slot in self._user_slots + self._bot_slots:
            slot.ready = False
        self._client_flood_until.clear()
        self._client_bad_until.clear()

    async def reconfigure(self):
        """Restart the Telegram client after the config wizard saved new credentials."""
        await self.stop()
        self.client = None
        self.user_client = None
        self.user_clients = []
        self.bot_client = None
        self.bot_clients = []
        self._user_slots = []
        self._bot_slots = []
        self._search_cache.clear()
        self._message_cache.clear()
        self._client_message_cache.clear()
        self._client_message_locks.clear()
        self._client_flood_until.clear()
        self._client_bad_until.clear()
        self._log_cache.clear()
        self._refresh_locks.clear()
        self._refresh_ts.clear()
        await self.start()

    def get_channel_ids(self) -> list:
        val = Config.TELEGRAM_CHANNEL_ID
        if not val:
            return []
        if isinstance(val, int):
            return [val]
        parts = [p.strip() for p in str(val).split(",")]
        ids = []
        for p in parts:
            if p.startswith("-") or p.isdigit():
                try:
                    ids.append(int(p))
                except ValueError:
                    ids.append(p)
            else:
                ids.append(p)
        return ids

    # ------------------------------------------------------------------
    # Anti-ban throttle: min spacing between Telegram search calls
    # ------------------------------------------------------------------
    async def _throttle(self):
        async with self._tg_spacing_lock:
            now = time.time()
            wait = self._last_tg_call + Config.TG_SEARCH_MIN_INTERVAL - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_tg_call = time.time()

    async def _respect_flood_wait(self):
        remaining = self._flood_wait_until - time.time()
        if remaining > 0:
            logger.warning(f"FloodWait backoff active, sleeping {remaining:.1f}s")
            await asyncio.sleep(min(remaining, Config.TG_FLOOD_WAIT_MAX))

    def _set_flood_wait(self, seconds: int):
        self._flood_wait_until = max(
            self._flood_wait_until,
            time.time() + min(max(1, seconds), Config.TG_FLOOD_WAIT_MAX),
        )

    def _set_client_flood_wait(self, client: Client, seconds: int):
        """FloodWait is per account: cool down one session, not the whole pool."""
        self._client_flood_until[id(client)] = max(
            self._client_flood_until.get(id(client), 0.0),
            time.time() + min(max(1, seconds), Config.TG_FLOOD_WAIT_MAX),
        )

    async def _respect_client_flood_wait(self, client: Client):
        remaining = self._client_flood_until.get(id(client), 0.0) - time.time()
        if remaining > 0:
            logger.warning(
                f"Telegram session cooldown active, sleeping {remaining:.1f}s "
                "(other sessions keep streaming)"
            )
            await asyncio.sleep(min(remaining, Config.TG_FLOOD_WAIT_MAX))

    # ------------------------------------------------------------------
    # Play logs to the configured Telegram log channel
    # ------------------------------------------------------------------
    def _local_now(self):
        import datetime
        from datetime import timezone, timedelta

        tz_str = getattr(Config, "TIMEZONE", "UTC") or "UTC"
        local_dt = None

        try:
            from zoneinfo import ZoneInfo
            local_dt = datetime.datetime.now(ZoneInfo(tz_str))
        except Exception:
            pass

        if local_dt is None:
            try:
                tz_clean = tz_str.upper().replace("UTC", "").replace("GMT", "").strip()
                if tz_clean and tz_clean[0] in ("+", "-"):
                    sign = 1 if tz_clean[0] == "+" else -1
                    time_parts = tz_clean[1:].split(":")
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1]) if len(time_parts) > 1 else 0
                    td = timedelta(hours=hours, minutes=minutes)
                    local_dt = datetime.datetime.now(timezone(sign * td))
            except Exception:
                pass

        if local_dt is None:
            local_dt = datetime.datetime.now(timezone.utc)
        return local_dt

    async def send_watch_log(self, event_type: str, filename: str, chat_id=None,
                             message_id=None, play_count: int = 1,
                             progress: str = "", extra: str = ""):
        """
        Send a play/watch event to the log channel with the owner's region time,
        date, year and how many times the file has been played.
        event_type: "play" | "start" | "seek" | "stop" | "finish"
        """
        if not Config.LOG_CHANNEL_ID:
            return
        if event_type in ("start", "seek", "stop", "finish") and not Config.WATCH_LOG_EVENTS:
            return

        key = (event_type, str(chat_id), str(message_id))
        now = time.time()

        # Avoid duplicate logs for the same file+event within 15 mins
        if key in self._log_cache and now - self._log_cache[key] < 900:
            return
        self._log_cache[key] = now

        # Keep the dedupe map bounded
        if len(self._log_cache) > 2048:
            cutoff = now - 1800
            self._log_cache = {k: v for k, v in self._log_cache.items() if v > cutoff}

        try:
            local_dt = self._local_now()
            time_str = local_dt.strftime("%Y-%m-%d %H:%M:%S")
            year_str = local_dt.strftime("%Y")
            tz_str = getattr(Config, "TIMEZONE", "UTC") or "UTC"

            icons = {"play": "▶️", "start": "🟢", "seek": "⏩", "stop": "🔴", "finish": "🏁"}
            titles = {
                "play": "Media Stream Log",
                "start": "Playback Started",
                "seek": "Seek Event",
                "stop": "Playback Stopped",
                "finish": "Watched to End",
            }
            icon = icons.get(event_type, "🎬")
            title = titles.get(event_type, "Watch Event")

            lines = [
                f"{icon} **{title}**",
                "",
                f"📁 **File:** `{filename}`",
                f"📅 **Date & Time:** `{time_str}` ({tz_str})",
                f"📆 **Year:** `{year_str}`",
                f"🔁 **Play Count:** `{play_count}`",
            ]
            if progress:
                lines.append(f"📊 **Progress:** `{progress}`")
            if chat_id is not None:
                lines.append(f"💬 **Source Channel:** `{chat_id}`")
            if message_id is not None:
                lines.append(f"🆔 **Message ID:** `{message_id}`")
            if extra:
                lines.append(f"ℹ️ {extra}")

            await self._active_client().send_message(
                chat_id=Config.LOG_CHANNEL_ID,
                text="\n".join(lines)
            )
        except Exception as e:
            logger.error(f"Failed to send log to log channel: {e}")

    async def send_play_log(self, filename: str, chat_id: Union[str, int], message_id: int):
        """Backwards compatible play log."""
        await self.send_watch_log("play", filename, chat_id, message_id, 1)

    # ------------------------------------------------------------------
    # Search (SQLite-persistent cache + anti-ban throttling)
    # ------------------------------------------------------------------
    async def search_messages(self, query: str = "", limit: int = 50):
        if not self.is_running:
            await self.start()

        if self.is_bot_only and not self._bot_only_warned:
            self._bot_only_warned = True
            logger.warning(
                "Bot-token mode: Telegram does not expose channel history search to "
                "bots. Catalogs/IMDb lookups work best with USER_SESSION_STRING."
            )

        query_str = str(query).strip() if query else ""

        cache_key = f"{query_str}:{limit}"
        now = time.time()

        # L1: in-memory
        if cache_key in self._search_cache:
            cached_time, cached_results = self._search_cache[cache_key]
            if now - cached_time < Config.CACHE_TTL:
                return cached_results

        # L2: persistent SQLite cache (survives restarts -> fewer Telegram calls)
        persisted = cache.search_cache_get(cache_key)
        if persisted is not None:
            restored = [cache.dict_to_msg(d) for d in persisted]
            self._search_cache[cache_key] = (now, restored)
            return restored

        # Limit concurrent full searches (single-level acquisition, no nesting)
        async with self._tg_semaphore:
            results = await self._search_telegram(query_str, limit, now, cache_key)

        if results is None:
            # FloodWait backoff was set; return whatever we have (possibly empty)
            return self._search_cache.get(cache_key, (now, []))[1]

        self._search_cache[cache_key] = (now, results)
        if len(self._search_cache) > 256:
            cutoff = now - Config.CACHE_TTL
            self._search_cache = {k: v for k, v in self._search_cache.items() if v[0] > cutoff}
        return results

    async def _search_telegram(self, query_str: str, limit: int, now: float, cache_key: str):
        chat_ids = self.get_channel_ids()
        results = []
        per_channel_limit = max(100, limit)

        try:
            client = self._active_client()
        except TelegramUnavailable as e:
            logger.error(f"Search skipped: {e}")
            return []

        await self._respect_flood_wait()

        for chat_id in chat_ids:
            try:
                await self._throttle()
                try:
                    async for msg in client.search_messages(chat_id=chat_id, query=query_str, limit=per_channel_limit) if query_str else client.get_chat_history(chat_id=chat_id, limit=per_channel_limit):
                        if self._has_media(msg):
                            results.append(msg)
                except FloodWait as e:
                    self._set_flood_wait(_flood_wait_seconds(e))
                    logger.warning(f"FloodWait {_flood_wait_seconds(e)}s hit during search - backing off")
                    return None
            except Exception as e:
                logger.warning(f"Telegram query failed for {chat_id}: {e}")

        try:
            results.sort(key=lambda m: m.date, reverse=True)
        except TypeError:
            pass

        # Resolve all split parts for detected split files to prevent missing segments
        split_bases = set()
        for msg in results:
            media = msg.video or msg.document or msg.audio
            if media:
                fn = getattr(media, "file_name", "") or msg.caption or ""
                base, part = parse_split_info(fn)
                if base:
                    search_query = re.sub(r'[^a-zA-Z0-9\s]', ' ', base)
                    search_query = re.sub(r'\s+', ' ', search_query).strip()
                    words = search_query.split()
                    if words and words[-1].lower() in ('mkv', 'mp4', 'avi', 'mov', 'wmv', 'flv', 'webm', 'ts', 'm4v', 'zip'):
                        words = words[:-1]
                    if len(words) > 5:
                        search_query = " ".join(words[:5])
                    else:
                        search_query = " ".join(words)
                    if search_query:
                        for chat_id in chat_ids:
                            split_bases.add((chat_id, search_query))

        additional_messages = []
        for chat_id, base in split_bases:
            try:
                logger.info(f"Fetching all split parts matching base: {base}")
                await self._throttle()
                try:
                    async for msg in client.search_messages(chat_id=chat_id, query=base, limit=100):
                        if self._has_media(msg):
                            additional_messages.append(msg)
                except FloodWait as e:
                    self._set_flood_wait(_flood_wait_seconds(e))
                    logger.warning(f"FloodWait {_flood_wait_seconds(e)}s hit during split-part search - backing off")
                    break
            except Exception as e:
                logger.warning(f"Failed to fetch additional split parts for {base}: {e}")

        # Merge and deduplicate by message ID
        deduped = {msg.id: msg for msg in results}
        for msg in additional_messages:
            deduped[msg.id] = msg

        final_results = list(deduped.values())
        try:
            final_results.sort(key=lambda m: m.date, reverse=True)
        except TypeError:
            pass
        final_results = final_results[:limit]

        # Persist to SQLite so restarts don't re-hit Telegram
        try:
            cache.search_cache_set(cache_key, [cache.msg_to_dict(m) for m in final_results], ttl=Config.CACHE_TTL)
        except Exception as e:
            logger.debug(f"Failed persisting search cache: {e}")

        return final_results

    # ------------------------------------------------------------------
    # Message fetch (persistent cache + live fetch for playback)
    # ------------------------------------------------------------------
    @staticmethod
    def _is_live_message(msg) -> bool:
        media = getattr(msg, "video", None) or getattr(msg, "document", None) or getattr(msg, "audio", None)
        return bool(media and getattr(media, "file_id", None))

    @staticmethod
    def _message_identity(message):
        chat_id = getattr(getattr(message, "chat", None), "id", None)
        message_id = getattr(message, "id", None)
        return chat_id, message_id

    def _cache_client_message(self, client: Client, message: Message):
        """Remember a live message for one account.

        File references are account-bound, so the same message fetched by two
        sessions must be cached separately.
        """
        chat_id, message_id = self._message_identity(message)
        if chat_id is None or message_id is None:
            return
        key = (id(client), int(chat_id), int(message_id))
        self._client_message_cache[key] = (time.time(), message)
        if len(self._client_message_cache) > 1024:
            cutoff = time.time() - max(300, Config.TG_STREAM_MESSAGE_TTL * 2)
            trimmed = {
                k: v for k, v in self._client_message_cache.items() if v[0] >= cutoff
            }
            if len(trimmed) > 1024:
                newest = sorted(trimmed.items(), key=lambda item: item[1][0], reverse=True)
                trimmed = dict(newest[:1024])
            self._client_message_cache = trimmed

    def _client_message_lock(self, key) -> asyncio.Lock:
        lock = self._client_message_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._client_message_locks[key] = lock
        return lock

    async def _get_message_for_client(self, client: Client, source: Message,
                                      force: bool = False) -> Message:
        """Return a live message whose file reference belongs to ``client``."""
        chat_id, message_id = self._message_identity(source)
        if chat_id is None or message_id is None:
            # No peer information to re-fetch with; the caller can only use the
            # message it already has.
            return source

        key = (id(client), int(chat_id), int(message_id))
        now = time.time()
        cached = self._client_message_cache.get(key)
        if cached and not force:
            age = now - cached[0]
            if age < Config.TG_STREAM_MESSAGE_TTL and self._is_live_message(cached[1]):
                return cached[1]

        # The active primary already fetched this live message (search results,
        # get_message, etc.). Its reference is valid for that account.
        if not force and self._is_live_message(source):
            try:
                if client is self._active_client():
                    self._cache_client_message(client, source)
                    return source
            except TelegramUnavailable:
                pass

        async with self._client_message_lock(key):
            cached = self._client_message_cache.get(key)
            if cached:
                age = time.time() - cached[0]
                if self._is_live_message(cached[1]) and (
                    (not force and age < Config.TG_STREAM_MESSAGE_TTL)
                    or (force and age < 2.0)
                ):
                    return cached[1]

            fetched = await client.get_messages(
                chat_id=chat_id, message_ids=message_id
            )
            if isinstance(fetched, list):
                fetched = fetched[0] if fetched else None
            if fetched is None:
                if self._is_live_message(source):
                    return source
                raise TelegramError(
                    f"Message {message_id} is not available to this Telegram session"
                )
            self._cache_client_message(client, fetched)
            return fetched

    async def _refresh_message_for_client(self, client: Client, message: Message) -> Message:
        """Re-fetch a message through one account to refresh its file reference."""
        refreshed = await self._get_message_for_client(client, message, force=True)
        if refreshed is not None:
            chat_id, message_id = self._message_identity(refreshed)
            if chat_id is not None and message_id is not None:
                self._refresh_ts[(chat_id, message_id)] = time.time()
        return refreshed

    async def get_message(self, message_id: int, chat_id: int = None,
                          live: bool = False, force: bool = False) -> Message:
        """Fetch a message.

        ``live=True`` guarantees a real Pyrogram message carrying a usable
        ``file_id``/``file_reference`` (the SQLite cache stores metadata only).
        Live messages are reused for ``TG_STREAM_MESSAGE_TTL`` seconds and the
        file reference is refreshed automatically by the streaming engine when
        Telegram reports it as expired.
        """
        if not self.is_running:
            await self.start()

        target_chat = chat_id if chat_id is not None else self.get_channel_ids()[0]
        cache_key = f"{target_chat}:{message_id}"
        now = time.time()

        cached = self._message_cache.get(cache_key)
        if cached and not force:
            cached_time, cached_msg = cached
            ttl = Config.TG_STREAM_MESSAGE_TTL if live else Config.CACHE_TTL
            if now - cached_time < ttl:
                if not live or self._is_live_message(cached_msg):
                    return cached_msg

        if not live and not force:
            # Persistent cache first (message metadata rarely changes)
            persisted = cache.message_cache_get(target_chat, message_id)
            if persisted is not None:
                self._message_cache[cache_key] = (now, persisted)
                return persisted

        try:
            client = self._active_client()
            msg = await client.get_messages(
                chat_id=target_chat, message_ids=message_id
            )
            if msg is not None:
                self._message_cache[cache_key] = (now, msg)
                self._cache_client_message(client, msg)
                try:
                    cache.message_cache_set(msg)
                except Exception:
                    pass
            return msg
        except Exception as e:
            logger.error(f"Failed to fetch message {message_id} in channel {target_chat}: {e}")
            raise e

    async def get_messages(self, message_ids: list, chat_id: int = None) -> list:
        """Fetch several live messages, batching the Telegram call where possible."""
        if not self.is_running:
            await self.start()
        target_chat = chat_id if chat_id is not None else self.get_channel_ids()[0]

        now = time.time()
        found = {}
        missing = []
        for msg_id in message_ids:
            cached = self._message_cache.get(f"{target_chat}:{msg_id}")
            if (cached and now - cached[0] < Config.TG_STREAM_MESSAGE_TTL
                    and self._is_live_message(cached[1])):
                found[msg_id] = cached[1]
            else:
                missing.append(msg_id)

        if missing:
            try:
                client = self._active_client()
                fetched = await client.get_messages(
                    chat_id=target_chat, message_ids=missing
                )
                if not isinstance(fetched, list):
                    fetched = [fetched]
                for msg in fetched:
                    if msg is None:
                        continue
                    found[msg.id] = msg
                    self._message_cache[f"{target_chat}:{msg.id}"] = (now, msg)
                    self._cache_client_message(client, msg)
                    try:
                        cache.message_cache_set(msg)
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Failed to fetch messages {missing} in channel {target_chat}: {e}")

        return [found.get(msg_id) for msg_id in message_ids]

    async def refresh_message(self, message: Message) -> Message:
        """Re-fetch a message through the primary client and update caches."""
        chat_id, message_id = self._message_identity(message)
        if chat_id is None or message_id is None:
            return message
        cache_key = f"{chat_id}:{message_id}"
        key = (chat_id, message_id)

        # Coalesce refreshes triggered by parallel prefetch tasks.
        cached = self._message_cache.get(cache_key)
        if cached and time.time() - cached[0] < 5 and self._is_live_message(cached[1]):
            return cached[1]

        lock = self._refresh_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._refresh_locks[key] = lock

        async with lock:
            cached = self._message_cache.get(cache_key)
            if cached and time.time() - cached[0] < 5 and self._is_live_message(cached[1]):
                return cached[1]
            client = self._active_client()
            refreshed = await self._refresh_message_for_client(client, message)
            if refreshed is not None:
                self._message_cache[cache_key] = (time.time(), refreshed)
                self._refresh_ts[key] = time.time()
                try:
                    cache.message_cache_set(refreshed)
                except Exception:
                    pass
            return refreshed or message

    # ------------------------------------------------------------------
    # Streaming engine (parallel prefetch + retry/refresh)
    # ------------------------------------------------------------------
    async def _fetch_chunk(self, client: Client, file_id: FileId,
                           file_size: int, index: int) -> bytes:
        """Read exactly one 1 MiB chunk at ``index`` from Telegram."""
        generator = client.get_file(file_id, file_size, limit=1, offset=index)
        try:
            async for chunk in generator:
                return chunk
        finally:
            # Explicitly close the generator so Pyrogram's transmission
            # semaphore is released before the next prefetch task starts.
            aclose = getattr(generator, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except asyncio.CancelledError:
                    raise
                except Exception as close_err:
                    logger.debug(f"Error closing chunk generator: {close_err}")
        return b""

    async def _ensure_stream_state(self, client: Client, source: Message,
                                   state: dict, file_size: int):
        """Populate one account's file id (file references are account-bound)."""
        if state.get("file_id") is not None:
            return
        async with state["lock"]:
            if state.get("file_id") is not None:
                return
            live = await self._get_message_for_client(client, source)
            media = (getattr(live, "video", None)
                     or getattr(live, "document", None)
                     or getattr(live, "audio", None))
            if not media or not getattr(media, "file_id", None):
                raise TelegramError(
                    f"Message {getattr(source, 'id', '?')} has no usable media "
                    f"for {self._client_label(client)}"
                )
            reported_size = int(getattr(media, "file_size", 0) or 0)
            if file_size and reported_size and reported_size != file_size:
                logger.warning(
                    f"{self._client_label(client)} reports a different size for "
                    f"message {getattr(source, 'id', '?')}: "
                    f"{reported_size} vs {file_size}"
                )
            state["message"] = live
            state["file_id"] = FileId.decode(media.file_id)

    def _state_for_client(self, client: Client, states: dict) -> dict:
        state = states.get(id(client))
        if state is None:
            state = {"message": None, "file_id": None, "lock": asyncio.Lock()}
            states[id(client)] = state
        return state

    async def _fetch_chunk_resilient(self, client: Client, message: Message,
                                     states: dict, file_size: int,
                                     index: int,
                                     alternatives: list = None) -> bytes:
        """Fetch one chunk, retrying and falling back to another account.

        A FloodWait on one account should not stall a whole stream when another
        session is available. Each account still uses its own live message and
        file reference (stored in ``states``).
        """
        candidates = [client]
        for other in alternatives or []:
            if other is not client:
                candidates.append(other)

        attempts = Config.TG_STREAM_MAX_RETRIES + 1
        last_exc = None

        for candidate_idx, candidate in enumerate(candidates):
            has_next = candidate_idx + 1 < len(candidates)
            state = self._state_for_client(candidate, states)
            label = self._client_label(candidate)

            now_ts = time.time()
            if has_next and (
                self._client_flood_until.get(id(candidate), 0.0) > now_ts
                or self._client_bad_until.get(id(candidate), 0.0) > now_ts
            ):
                logger.info(
                    f"{label} is cooling down; trying another session for "
                    f"message {getattr(message, 'id', '?')} chunk {index}"
                )
                continue

            for attempt in range(attempts):
                try:
                    await self._ensure_stream_state(candidate, message, state, file_size)
                    return await self._fetch_chunk(
                        candidate, state["file_id"], file_size, index
                    )
                except asyncio.CancelledError:
                    raise
                except FloodWait as e:
                    wait = _flood_wait_seconds(e)
                    self._set_client_flood_wait(candidate, wait)
                    last_exc = e
                    logger.warning(
                        f"{label} hit FloodWait {wait}s for message "
                        f"{getattr(message, 'id', '?')} chunk {index}"
                    )
                    if has_next:
                        break
                except Exception as e:
                    last_exc = e
                    if _is_access_error(e) and not _is_file_reference_error(e):
                        # This account cannot read the chat (e.g. a bot that is
                        # not an admin): park it and let another one carry on.
                        self._client_bad_until[id(candidate)] = time.time() + 300
                        logger.warning(
                            f"{label} cannot read message {getattr(message, 'id', '?')}: "
                            f"{e}; skipping it for 5 minutes"
                        )
                        if has_next:
                            break
                        raise StreamingError(
                            f"Telegram read failed for message {getattr(message, 'id', '?')} "
                            f"chunk {index}: {e}"
                        )
                    if _is_file_reference_error(e):
                        state["file_id"] = None
                        try:
                            refreshed = await self._refresh_message_for_client(candidate, message)
                            media = (getattr(refreshed, "video", None)
                                     or getattr(refreshed, "document", None)
                                     or getattr(refreshed, "audio", None))
                            if media and getattr(media, "file_id", None):
                                state["message"] = refreshed
                                state["file_id"] = FileId.decode(media.file_id)
                            logger.info(
                                f"{label} refreshed expired file reference for "
                                f"message {getattr(message, 'id', '?')}"
                            )
                        except Exception as refresh_err:
                            logger.warning(
                                f"{label} failed to refresh file reference for "
                                f"message {getattr(message, 'id', '?')}: {refresh_err}"
                            )
                    elif not _is_retryable_error(e):
                        if has_next:
                            break
                        raise

                if attempt + 1 < attempts:
                    delay = min(8.0, Config.TG_STREAM_RETRY_BACKOFF * (2 ** attempt))
                    if isinstance(last_exc, FloodWait):
                        await self._respect_client_flood_wait(candidate)
                    else:
                        await asyncio.sleep(delay)

        raise StreamingError(
            f"Telegram read failed for message {getattr(message, 'id', '?')} "
            f"chunk {index}: {last_exc}"
        )

    async def fetch_block(self, message: Message, block_index: int) -> bytes:
        """Fetch one 1 MiB block for ZIP readers (retries + reference refresh)."""
        media = getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "audio", None)
        if not media:
            return b""
        file_size = int(getattr(media, "file_size", 0) or 0)
        start = block_index * CHUNK_SIZE
        if file_size <= 0 or start >= file_size:
            return b""
        end = min(file_size - 1, start + CHUNK_SIZE - 1)
        data = bytearray()
        async for chunk in self.iter_media_range(message, start, end):
            data.extend(chunk)
        return bytes(data)

    async def iter_media_range(self, message: Message, start: int, end: int,
                               client: Client = None) -> AsyncGenerator[bytes, None]:
        """Yield bytes ``start..end`` (inclusive) with a parallel read-ahead window.

        Chunks are fetched in order but several 1 MiB requests stay in flight,
        which is what keeps high-bitrate files from stalling on a single
        request/response round trip. When more than one user session is
        configured, consecutive chunks rotate across accounts (each account
        gets its own live message/file reference). Expired references are
        refreshed transparently and FloodWait is honoured per session.
        """
        media = getattr(message, "video", None) or getattr(message, "document", None) or getattr(message, "audio", None)
        if not media or not getattr(media, "file_id", None):
            # Callers may hand us a metadata-only CachedMessage (e.g. from the
            # persistent search cache). Upgrade it to a live message first.
            chat = getattr(message, "chat", None)
            msg_id = getattr(message, "id", None)
            if chat is not None and msg_id is not None:
                try:
                    live_msg = await self.get_message(msg_id, chat_id=chat.id, live=True)
                    if live_msg is not None:
                        message = live_msg
                        media = (getattr(message, "video", None)
                                 or getattr(message, "document", None)
                                 or getattr(message, "audio", None))
                except Exception as e:
                    logger.warning(
                        f"Could not upgrade cached message {msg_id} for streaming: {e}"
                    )
        if not media or not getattr(media, "file_id", None):
            raise TelegramError("Message does not contain a streamable media file")

        file_size = int(getattr(media, "file_size", 0) or 0)
        if file_size <= 0:
            raise TelegramError("Telegram did not report a valid file size")

        start = max(0, int(start))
        end = min(int(end), file_size - 1)
        if start > end:
            return

        if client is not None:
            clients = [client]
            stream_offset = 0
        else:
            chat_id, _ = self._message_identity(message)
            clients = self._stream_clients(file_size, chat_id=chat_id)
            stream_offset = self._next_stream_offset(len(clients))

        states = {}

        chunk_size = CHUNK_SIZE
        first_chunk = start // chunk_size
        last_chunk = end // chunk_size
        prefetch = max(1, Config.TG_STREAM_PREFETCH)

        pending = {}

        def client_for_chunk(index: int) -> Client:
            if len(clients) == 1:
                return clients[0]
            offset = (stream_offset + (index - first_chunk)) % len(clients)
            return clients[offset]

        def schedule(index: int):
            if index > last_chunk or index in pending:
                return
            pending[index] = asyncio.create_task(
                self._fetch_chunk_resilient(
                    client_for_chunk(index), message, states, file_size, index,
                    alternatives=clients,
                )
            )

        next_index = first_chunk
        try:
            while next_index <= last_chunk and len(pending) < prefetch:
                schedule(next_index)
                next_index += 1

            for index in range(first_chunk, last_chunk + 1):
                task = pending.pop(index, None)
                if task is None:
                    task = asyncio.create_task(
                        self._fetch_chunk_resilient(
                            client_for_chunk(index), message, states, file_size, index,
                            alternatives=clients,
                        )
                    )

                # Keep the window full while the current chunk downloads.
                while next_index <= last_chunk and len(pending) < prefetch:
                    schedule(next_index)
                    next_index += 1

                chunk = await task
                if not chunk:
                    break

                chunk_start = index * chunk_size
                lo = max(0, start - chunk_start)
                hi = min(len(chunk), end - chunk_start + 1)
                if hi > lo:
                    yield chunk[lo:hi]
        finally:
            for task in pending.values():
                task.cancel()
            if pending:
                await asyncio.gather(*pending.values(), return_exceptions=True)

    def _has_media(self, msg) -> bool:
        return bool(msg.video or msg.document or msg.audio)

tg_client_manager = TelegramClientManager()
