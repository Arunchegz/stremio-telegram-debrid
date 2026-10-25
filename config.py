import os
import re

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import config_store


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        return float(default)


def _to_bool(value) -> bool:
    """Coerce env/file values to bool without bool("false") being True."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


class Config:
    PORT = int(os.getenv("PORT", 7860))
    ADDON_URL = os.getenv("ADDON_URL", f"http://localhost:{PORT}").rstrip("/")
    API_KEY = os.getenv("API_KEY", "")
    CACHE_TTL = _env_int("CACHE_TTL", 1800)
    TIMEZONE = os.getenv("TIMEZONE", "UTC")

    API_ID = os.getenv("API_ID")
    API_HASH = os.getenv("API_HASH")
    # BOT_TOKEN is the primary bot. BOT_TOKENS may hold extra bots (one per line
    # or comma separated); BOT_TOKEN_2, _3, ... work too. Every connected bot
    # joins the streaming rotation next to the user sessions.
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    BOT_TOKENS = os.getenv("BOT_TOKENS", "")
    # USER_SESSION_STRING is the primary session. USER_SESSION_STRINGS may hold
    # extra sessions (one per line or comma separated) that are rotated across
    # streaming requests. Numbered env vars (USER_SESSION_STRING_2, _3, ...)
    # are accepted too.
    USER_SESSION_STRING = os.getenv("USER_SESSION_STRING", "")
    USER_SESSION_STRINGS = os.getenv("USER_SESSION_STRINGS", "")

    # auto = use the session string(s) when they work, keep the bot as fallback.
    # user / bot force a single credential type.
    TG_AUTH_MODE = os.getenv("TG_AUTH_MODE", "auto").strip().lower()

    # When true (default) bot clients carry stream chunks together with the
    # user sessions. When false, bots are only a fallback if every user
    # session fails (the old behaviour).
    TG_BOT_STREAMING = _env_bool("TG_BOT_STREAMING", "true")

    TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
    LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID")

    # --- New: metadata / localization ---
    TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
    TMDB_LANGUAGE = os.getenv("TMDB_LANGUAGE", "en-US")

    # --- Rate limiting (disabled by default; owner can enable) ---
    RATE_LIMIT_ENABLED = _env_bool("RATE_LIMIT_ENABLED", "false")
    RATE_LIMIT_REQUESTS = _env_int("RATE_LIMIT_REQUESTS", 120)       # JSON API reqs / window / IP
    RATE_LIMIT_WINDOW = _env_int("RATE_LIMIT_WINDOW", 60)            # seconds
    RATE_LIMIT_STREAM_REQUESTS = _env_int("RATE_LIMIT_STREAM_REQUESTS", 600)  # media range reqs / window / IP
    RATE_LIMIT_BURST = _env_int("RATE_LIMIT_BURST", 30)              # API burst allowance
    RATE_LIMIT_STREAM_BURST = _env_int("RATE_LIMIT_STREAM_BURST", 120)  # playback burst allowance
    RATE_LIMIT_STREAM_CONCURRENCY = _env_int("RATE_LIMIT_STREAM_CONCURRENCY", 8)  # simultaneous streams / IP
    TRUST_PROXY_HEADERS = _env_bool("TRUST_PROXY_HEADERS", "true")

    # --- Telegram streaming engine ---
    # Number of 1 MiB chunks fetched ahead per stream (also the client's max
    # concurrent transmissions, i.e. the global per-client download window).
    TG_STREAM_PREFETCH = _env_int("TG_STREAM_PREFETCH", 4)
    TG_STREAM_MAX_RETRIES = _env_int("TG_STREAM_MAX_RETRIES", 3)
    TG_STREAM_RETRY_BACKOFF = _env_float("TG_STREAM_RETRY_BACKOFF", 0.5)
    TG_STREAM_MESSAGE_TTL = _env_int("TG_STREAM_MESSAGE_TTL", 900)  # reuse live message refs (seconds); Telegram references expire in ~60 min
    # Pyrogram talks MTProto, so bots can stream large files (the 20 MB cap
    # belongs to the HTTP Bot API). Optional client-side guard for bot-only
    # setups; 0 (default) disables it.
    BOT_FILE_LIMIT_MB = _env_int("BOT_FILE_LIMIT_MB", 0)

    # --- Telegram API anti-flood (search) ---
    TG_SEARCH_CONCURRENCY = _env_int("TG_SEARCH_CONCURRENCY", 2)
    TG_SEARCH_MIN_INTERVAL = _env_float("TG_SEARCH_MIN_INTERVAL", 0.35)
    TG_FLOOD_WAIT_MAX = _env_int("TG_FLOOD_WAIT_MAX", 120)

    # --- Watch history / play logs ---
    WATCH_LOG_EVENTS = _env_bool("WATCH_LOG_EVENTS", "true")              # log start/seek/stop events

    # --- Wizard password (plaintext via env; hashed in config file) ---
    CONFIG_PASSWORD = os.getenv("CONFIG_PASSWORD", "")
    DISABLE_WIZARD = _env_bool("DISABLE_WIZARD", "false")


    @classmethod
    def session_strings(cls) -> list:
        """Return unique user-session strings in priority order.

        Accepts newline/comma/semicolon separated values in
        ``USER_SESSION_STRING``/``USER_SESSION_STRINGS`` plus numbered env vars
        such as ``USER_SESSION_STRING_2``. Session strings themselves never
        contain whitespace or those separators, so splitting is safe.
        """
        blobs = [
            getattr(cls, "USER_SESSION_STRING", "") or "",
            getattr(cls, "USER_SESSION_STRINGS", "") or "",
        ]
        for index in range(2, 9):
            blobs.append(os.getenv(f"USER_SESSION_STRING_{index}", "") or "")

        sessions = []
        seen = set()
        for blob in blobs:
            for token in re.split(r"[\s,;]+", str(blob).strip()):
                token = token.strip()
                if token and token not in seen:
                    seen.add(token)
                    sessions.append(token)
        return sessions

    @classmethod
    def bot_tokens(cls) -> list:
        """Return unique bot tokens in priority order.

        Accepts ``BOT_TOKEN`` plus newline/comma/semicolon separated
        ``BOT_TOKENS`` and numbered ``BOT_TOKEN_2`` ... ``BOT_TOKEN_8``.
        Bot tokens look like ``123456:ABC-DEF`` and never contain whitespace
        or those separators.
        """
        blobs = [
            getattr(cls, "BOT_TOKEN", "") or "",
            getattr(cls, "BOT_TOKENS", "") or "",
        ]
        for index in range(2, 9):
            blobs.append(os.getenv(f"BOT_TOKEN_{index}", "") or "")

        tokens = []
        seen = set()
        for blob in blobs:
            for token in re.split(r"[\s,;]+", str(blob).strip()):
                token = token.strip()
                if token and token not in seen:
                    seen.add(token)
                    tokens.append(token)
        return tokens

    @classmethod
    def apply_file_overrides(cls):
        """Overlay values saved through the /configure wizard (file > env)."""
        saved = config_store.load_file_config()
        for key, value in saved.items():
            if key == "CONFIG_PASSWORD":
                # env password still works alongside the stored hash
                if value:
                    cls.CONFIG_PASSWORD = value
                continue
            if hasattr(cls, key):
                setattr(cls, key, value)
        cls.normalize()

    @classmethod
    def normalize(cls):
        """Normalize channel ids / ints / bools after any config change."""
        try:
            cls.API_ID = int(cls.API_ID)
        except (ValueError, TypeError):
            pass

        for attr in ("TELEGRAM_CHANNEL_ID", "LOG_CHANNEL_ID"):
            val = getattr(cls, attr, None)
            if val and isinstance(val, str):
                stripped = val.strip()
                if stripped.startswith("-") or stripped.isdigit():
                    try:
                        setattr(cls, attr, int(stripped))
                    except ValueError:
                        pass

        cls.TG_AUTH_MODE = str(getattr(cls, "TG_AUTH_MODE", "auto") or "auto").strip().lower()
        if cls.TG_AUTH_MODE not in ("auto", "user", "bot"):
            cls.TG_AUTH_MODE = "auto"

        for attr, default in (
            ("RATE_LIMIT_REQUESTS", 120), ("RATE_LIMIT_WINDOW", 60),
            ("RATE_LIMIT_STREAM_REQUESTS", 600), ("RATE_LIMIT_BURST", 30),
            ("RATE_LIMIT_STREAM_BURST", 120), ("RATE_LIMIT_STREAM_CONCURRENCY", 8),
            ("TG_STREAM_PREFETCH", 4), ("TG_STREAM_MAX_RETRIES", 3),
            ("TG_STREAM_MESSAGE_TTL", 240), ("BOT_FILE_LIMIT_MB", 0),
            ("TG_SEARCH_CONCURRENCY", 2), ("TG_FLOOD_WAIT_MAX", 120),
        ):
            try:
                value = int(getattr(cls, attr, default))
            except (TypeError, ValueError):
                value = default
            setattr(cls, attr, max(0, value))

        # A stream needs at least one concurrent transmission; clamp to a sane cap.
        cls.TG_STREAM_PREFETCH = min(max(1, cls.TG_STREAM_PREFETCH), 16)
        cls.TG_STREAM_MAX_RETRIES = min(cls.TG_STREAM_MAX_RETRIES, 10)
        cls.TG_SEARCH_CONCURRENCY = min(max(1, cls.TG_SEARCH_CONCURRENCY), 8)
        cls.RATE_LIMIT_WINDOW = max(1, cls.RATE_LIMIT_WINDOW)
        cls.TG_STREAM_MESSAGE_TTL = max(30, cls.TG_STREAM_MESSAGE_TTL)

        try:
            cls.TG_STREAM_RETRY_BACKOFF = max(0.0, float(getattr(cls, "TG_STREAM_RETRY_BACKOFF", 0.5)))
        except (TypeError, ValueError):
            cls.TG_STREAM_RETRY_BACKOFF = 0.5
        try:
            cls.TG_SEARCH_MIN_INTERVAL = max(0.0, float(getattr(cls, "TG_SEARCH_MIN_INTERVAL", 0.35)))
        except (TypeError, ValueError):
            cls.TG_SEARCH_MIN_INTERVAL = 0.35

        cls.RATE_LIMIT_ENABLED = _to_bool(cls.RATE_LIMIT_ENABLED)
        cls.TG_BOT_STREAMING = _to_bool(getattr(cls, "TG_BOT_STREAMING", True))
        cls.WATCH_LOG_EVENTS = _to_bool(cls.WATCH_LOG_EVENTS)
        cls.TRUST_PROXY_HEADERS = _to_bool(cls.TRUST_PROXY_HEADERS)

    @classmethod
    def validate(cls):
        cls.TG_AUTH_MODE = str(getattr(cls, "TG_AUTH_MODE", "auto") or "auto").strip().lower()
        if cls.TG_AUTH_MODE not in ("auto", "user", "bot"):
            cls.TG_AUTH_MODE = "auto"

        sessions = cls.session_strings()
        bots = cls.bot_tokens()
        missing = []
        if not cls.API_ID:
            missing.append("API_ID")
        if not cls.API_HASH:
            missing.append("API_HASH")
        if cls.TG_AUTH_MODE == "user":
            if not sessions:
                missing.append("USER_SESSION_STRING (TG_AUTH_MODE=user)")
        elif cls.TG_AUTH_MODE == "bot":
            if not bots:
                missing.append("BOT_TOKEN (TG_AUTH_MODE=bot)")
        elif not bots and not sessions:
            missing.append("BOT_TOKEN or USER_SESSION_STRING")
        if not cls.TELEGRAM_CHANNEL_ID:
            missing.append("TELEGRAM_CHANNEL_ID")

        if missing:
            raise ValueError(
                f"Missing critical configuration variables: {', '.join(missing)}. "
                "Run the configuration wizard at /configure or set them in your environment."
            )

        try:
            cls.API_ID = int(cls.API_ID)
        except (ValueError, TypeError):
            raise ValueError("API_ID must be a valid integer.")

        cls.normalize()
