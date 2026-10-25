import re
import logging
import unicodedata
import urllib.parse

import httpx

logger = logging.getLogger("utils")

def format_size(bytes_size: int) -> str:
    if not bytes_size:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while bytes_size >= 1024 and i < len(units) - 1:
        bytes_size /= 1024.0
        i += 1
    return f"{bytes_size:.2f} {units[i]}"

_NORM_MAP = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    "uno": "1", "dos": "2", "tres": "3", "cuatro": "4", "cinco": "5",
    "seis": "6", "siete": "7", "ocho": "8", "nueve": "9", "diez": "10",
    "temporada": "season", "temp": "season", "capitulo": "episode", 
    "capítulo": "episode", "cap": "episode", "ep": "episode", "ch": "episode", "chapter": "episode"
}

def _normalize_filename(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r'[._\-]', ' ', re.sub(r'\.[a-z0-9]{2,5}$', '', text.lower()))
    return " ".join(_NORM_MAP.get(w, w) for w in t.split())

_SEASON_EPISODE_PATTERNS = [
    re.compile(r'\bs\s*(?P<s>\d{1,2})\s*[.\-_ ]?\s*e\s*(?P<e>\d{1,3})\b', re.IGNORECASE),
    re.compile(r'\bseason\s*(?P<s>\d{1,2})\D{0,10}?episode\s*(?P<e>\d{1,3})\b', re.IGNORECASE),
    re.compile(r'\bt\s*(?P<s>\d{1,2})\s*[.\-_ ]?\s*c\s*(?P<e>\d{1,3})\b', re.IGNORECASE),
    re.compile(r'(?<!\d)(?P<s>\d{1,2})\s*[xX]\s*(?P<e>\d{1,3})(?!\d)'),
]

_EPISODE_SEASON_PATTERNS = [
    re.compile(r'\be\s*(?P<e>\d{1,3})\s*[.\-_ xX]?\s*s\s*(?P<s>\d{1,2})\b', re.IGNORECASE),
    re.compile(r'\bepisode\s*(?P<e>\d{1,3})\D{0,10}?season\s*(?P<s>\d{1,2})\b', re.IGNORECASE),
    re.compile(r'\bc\s*(?P<e>\d{1,3})\s*[.\-_ ]?\s*t\s*(?P<s>\d{1,2})\b', re.IGNORECASE),
]

# "S3-22", "[S3-22]", "S03 - 22", "S3–22" (hyphen / en dash / em dash between season and episode).
# Matched against the RAW filename because _normalize_filename() strips hyphens.
_SEASON_DASH_EPISODE_PATTERN = re.compile(
    r'(?<![a-z0-9])s(?P<s>\d{1,2})\s*[-\u2013\u2014]\s*(?P<e>\d{1,4})(?![a-z0-9])',
    re.IGNORECASE,
)

# "10. Cyberpunk Edgerunners [T4TSA.cc].mkv", "01) Show", "05 - Show": episode number at the very
# start of the filename. Matched against the RAW filename (normalisation removes the "." / "-").
_LEADING_EPISODE_PATTERN = re.compile(
    r'^\s*(?P<e>\d{1,3})\s*(?:\.\s+|\)\s+|\s+[-\u2013\u2014]\s+)(?=\S)'
)

_STANDALONE_EPISODE_PATTERNS = [
    re.compile(r'\bep(?:isode)?\s*[.\-_ ]?\s*(?P<e>\d{1,3})\b', re.IGNORECASE),
    re.compile(r'\bcap(?:itulo|ítulo)?\s*[.\-_ ]?\s*(?P<e>\d{1,3})\b', re.IGNORECASE),
    re.compile(r'\[(?P<e>\d{2,3})\]'),
    re.compile(r'(?:^|[\s\-_])[-–]\s*(?P<e>\d{2,3})\s*(?:[-–]|$|\.)'),
]

def parse_season_episode(filename: str) -> tuple:
    if not filename:
        return None, None
    
    fn = _normalize_filename(filename)
    for pat in _SEASON_EPISODE_PATTERNS + _EPISODE_SEASON_PATTERNS:
        m = pat.search(fn)
        if m:
            try:
                return int(m.group('s')), int(m.group('e'))
            except (ValueError, KeyError, IndexError):
                pass
                
    m = _SEASON_DASH_EPISODE_PATTERN.search(filename)
    if m:
        try:
            return int(m.group('s')), int(m.group('e'))
        except (ValueError, KeyError, IndexError):
            pass

    for pat in _STANDALONE_EPISODE_PATTERNS:
        m = pat.search(fn)
        if m:
            try:
                return 1, int(m.group('e'))
            except (ValueError, KeyError, IndexError):
                pass

    m = _LEADING_EPISODE_PATTERN.search(filename)
    if m:
        try:
            return 1, int(m.group('e'))
        except (ValueError, KeyError, IndexError):
            pass

    return None, None

def matches_episode(filename: str, season: int, episode: int) -> bool:
    if season is None or episode is None:
        return True
        
    f_season, f_episode = parse_season_episode(filename)
    if f_season == season and f_episode == episode:
        return True
        
    fn = _normalize_filename(filename)
    patterns = [
        rf'\bs\s*{season:02d}\s*[.\-_ ]?\s*e\s*{episode:02d}\b',
        rf'\bs\s*{season}\s*[.\-_ ]?\s*e\s*{episode:02d}\b',
        rf'(?<!\d){season}[xX]{episode:02d}(?!\d)',
        rf'(?<!\d){season}[xX]{episode}(?!\d)',
        rf'\[season\s*0*{season}\].*?\[episode\s*0*{episode}\]',
        rf'season\s*0*{season}\D{{0,20}}?episode\s*0*{episode}(?!\d)',
        rf'\bt\s*{season:02d}\s*c\s*{episode:02d}\b',
        rf'\bt\s*{season}\s*c\s*{episode}\b',
        rf'(?<!\d){season}{episode:02d}(?!\d)',
    ]
    
    has_explicit_season = any(re.search(p, fn, re.IGNORECASE) for p in [r'\bs\d', r'\bseason\s*\d', r'\bt\d', r'\d+[xX]'])
    if season == 1 and not has_explicit_season:
        patterns += [
            rf'\bepisode\s*0*{episode}\b',
            rf'\bcap\s*0*{episode}\b',
            rf'\[0*{episode}\]',
            rf'[-–]\s*0*{episode:02d}\s*(?:[-–]|$)',
        ]
    
    return any(re.search(pat, fn, re.IGNORECASE) for patpat in patterns for pat in [patpat])

def matches_subtitle(video_filename: str, sub_filename: str) -> bool:
    if not video_filename or not sub_filename:
        return False
        
    v_base = video_filename.lower().rsplit('.', 1)[0]
    s_base = sub_filename.lower().rsplit('.', 1)[0]
    s_base_clean = re.sub(r'\.(eng|en|english|sub|subtitle|srt|vtt)$', '', s_base)
    
    return s_base_clean in v_base or v_base in s_base_clean


_SUB_LANG_MAP = {
    "eng": "eng", "english": "eng", "en": "eng",
    "spa": "spa", "spanish": "spa", "es": "spa", "esp": "spa", "castellano": "spa",
    "fre": "fre", "french": "fre", "fr": "fre", "fra": "fre",
    "ger": "ger", "german": "ger", "de": "ger", "deu": "ger",
    "ita": "ita", "italian": "ita", "it": "ita",
    "por": "por", "portuguese": "por", "pt": "por", "pob": "por",
    "hin": "hin", "hindi": "hin", "hi": "hin",
    "jpn": "jpn", "japanese": "jpn", "ja": "jpn",
    "kor": "kor", "korean": "kor", "ko": "kor",
    "chi": "chi", "chinese": "chi", "zh": "chi", "zho": "chi",
    "ara": "ara", "arabic": "ara", "ar": "ara",
    "rus": "rus", "russian": "rus", "ru": "rus",
    "tur": "tur", "turkish": "tur", "tr": "tur",
    "dut": "dut", "dutch": "dut", "nl": "dut", "nld": "dut",
    "pol": "pol", "polish": "pol", "pl": "pol",
    "swe": "swe", "swedish": "swe", "sv": "swe",
    "nor": "nor", "norwegian": "nor", "no": "nor",
    "fin": "fin", "finnish": "fin", "fi": "fin",
    "dan": "dan", "danish": "dan", "da": "dan",
    "tam": "tam", "tamil": "tam", "ta": "tam",
    "tel": "tel", "telugu": "tel", "te": "tel",
    "mal": "mal", "malayalam": "mal", "ml": "mal",
    "ben": "ben", "bengali": "ben", "bn": "ben",
    "ind": "ind", "indonesian": "ind", "id": "ind",
    "tha": "tha", "thai": "tha", "th": "tha",
    "vie": "vie", "vietnamese": "vie", "vi": "vie",
    "ukr": "ukr", "ukrainian": "ukr", "uk": "ukr",
    "cze": "cze", "czech": "cze", "cs": "cze", "ces": "cze",
    "ell": "gre", "greek": "gre", "el": "gre", "gre": "gre",
    "heb": "heb", "hebrew": "heb", "he": "heb",
    "hun": "hun", "hungarian": "hun", "hu": "hun",
    "rum": "rum", "romanian": "rum", "ro": "rum", "ron": "rum",
}


def detect_subtitle_language(filename: str) -> str:
    """Detect ISO 639-2 language code from subtitle filename."""
    if not filename:
        return "eng"
    name = re.sub(r'\.(srt|vtt|ass|ssa|sub)$', '', filename.lower())
    # Split filename tokens by punctuation/spaces
    tokens = [t for t in re.split(r'[\s._\-–—()\[\]{}]+', name) if t]
    for token in reversed(tokens):
        if token in _SUB_LANG_MAP:
            return _SUB_LANG_MAP[token]
    return "eng"

def get_search_query_from_filename(filename: str) -> str:
    if not filename:
        return ""
    name = re.sub(r'[._\-]', ' ', filename.lower().rsplit('.', 1)[0])
    terms = r'\b(2160p|1080p|720p|480p|360p|4k|8k|10bit|h264|x264|h265|x265|hevc|web[- ]?rip|bluray|brrip|hdrip)\b'
    match = re.search(terms, name)
    if match:
        name = name[:match.start()]
    
    return re.sub(r'\s+', ' ', name).strip()

def parse_split_info(filename: str) -> tuple:
    if not filename:
        return None, None
        
    m1 = re.search(r'\.(\d{3,4})$', filename)
    if m1:
        return filename[:m1.start()], int(m1.group(1))
        
    m2 = re.search(r'[._\- ]part_?(\d+)(?:\.([^.]+))?$', filename, re.IGNORECASE)
    if m2:
        part = int(m2.group(1))
        ext = m2.group(2) or ""
        base = filename[:m2.start()] + (f".{ext}" if ext else "")
        return base, part
        
    return None, None

def parse_http_range(range_header: str, total_size: int):
    """Parse a single-range HTTP ``Range`` header.

    Returns ``None`` when the header is absent, a ``(start, end)`` tuple with an
    inclusive end otherwise. Raises ``ValueError`` for malformed, multi-range or
    unsatisfiable values (callers should answer those with 416).
    """
    if not range_header:
        return None
    if total_size <= 0:
        raise ValueError("empty resource")

    value = range_header.strip()
    if not value.lower().startswith("bytes="):
        raise ValueError("unsupported range unit")

    spec = value[len("bytes="):].strip()
    if not spec or "," in spec:
        raise ValueError("only a single byte range is supported")

    start_raw, sep, end_raw = spec.partition("-")
    if not sep:
        raise ValueError("invalid range")

    start_raw = start_raw.strip()
    end_raw = end_raw.strip()

    if not start_raw:
        # Suffix range: bytes=-N (last N bytes)
        if not end_raw.isdigit():
            raise ValueError("invalid suffix range")
        suffix = int(end_raw)
        if suffix <= 0:
            raise ValueError("invalid suffix range")
        start = max(0, total_size - suffix)
        end = total_size - 1
    else:
        if not start_raw.isdigit() or (end_raw and not end_raw.isdigit()):
            raise ValueError("invalid range bounds")
        start = int(start_raw)
        if start >= total_size:
            raise ValueError("range starts past end of resource")
        end = int(end_raw) if end_raw else total_size - 1
        if end < start:
            raise ValueError("range end before start")
        end = min(end, total_size - 1)

    return start, end


def content_disposition(filename: str, inline: bool = True) -> str:
    """Build a header-safe Content-Disposition value for arbitrary filenames."""
    name = (filename or "download").replace("\\", "/").rsplit("/", 1)[-1]
    # Keep the quoted ASCII fallback free of characters that break the header.
    safe = "".join(ch for ch in name if ch.isprintable() and ch not in '"\\\r\n')
    fallback = safe.encode("ascii", "ignore").decode("ascii").strip() or "download"
    encoded = urllib.parse.quote(name, safe="")
    kind = "inline" if inline else "attachment"
    return f"{kind}; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"


async def _get_shared_http_client() -> httpx.AsyncClient:
    """Return the shared httpx client from metadata.py (connection reuse)."""
    try:
        from metadata import _get_client
        return await _get_client()
    except Exception:
        # Fallback: import failed or client not ready yet
        return httpx.AsyncClient(timeout=8.0, follow_redirects=True)


async def get_metadata_from_cinemeta(meta_type: str, imdb_id: str) -> dict:
    cache_key = f"cinemeta:{meta_type}:{imdb_id}"

    # Check persistent SQLite cache first (survives restarts, auto-expires)
    try:
        import cache as _cache
        cached = _cache.cache_get("meta", cache_key)
        if cached is not None:
            return cached
    except Exception:
        pass

    url = f"https://v3-cinemeta.strem.io/meta/{meta_type}/{imdb_id}.json"
    logger.info(f"Fetching metadata from Cinemeta: {url}")

    try:
        client = await _get_shared_http_client()
        resp = await client.get(url)
        if resp.status_code == 200:
            meta = resp.json().get("meta", {})
            if meta:
                result = {
                    "name": meta.get("name"),
                    "year": meta.get("year"),
                    "genres": meta.get("genres", []),
                    "poster": meta.get("poster")
                }
                # Persist to SQLite (1-day TTL) — bounded, survives restarts
                try:
                    _cache.cache_set("meta", cache_key, result, ttl=86400)
                except Exception:
                    pass
                return result
    except Exception as e:
        logger.error(f"Cinemeta metadata lookup failed: {e}")

    return {}

VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ts', '.m4v')

def is_video_file(filename: str) -> bool:
    return filename.lower().endswith(VIDEO_EXTENSIONS)

_ROMAN_NUMS = [
    (r'\bx\b', '10'), (r'\bix\b', '9'), (r'\bviii\b', '8'), (r'\bvii\b', '7'),
    (r'\bvi\b', '6'), (r'\bv\b', '5'), (r'\biv\b', '4'), (r'\biii\b', '3'), (r'\bii\b', '2')
]

def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = "".join(c for c in unicodedata.normalize('NFD', title) if unicodedata.category(c) != 'Mn').lower()
    t = re.sub(r'[^a-z0-9\s]', ' ', t)
    for pat, rep in _ROMAN_NUMS:
        t = re.sub(pat, rep, t)
    return re.sub(r'\s+', ' ', t).strip()

def _clean_title_prefix(filename: str) -> str:
    if not filename:
        return ""
    fn_lower = filename.lower()
    first_match_idx = len(filename)
    
    all_patterns = _SEASON_EPISODE_PATTERNS + _EPISODE_SEASON_PATTERNS + _STANDALONE_EPISODE_PATTERNS
    for pat in all_patterns:
        m = pat.search(fn_lower)
        if m:
            first_match_idx = min(first_match_idx, m.start())
            
    m = _SEASON_DASH_EPISODE_PATTERN.search(filename)
    if m:
        first_match_idx = min(first_match_idx, m.start())

    m = _LEADING_EPISODE_PATTERN.search(filename)
    if m:
        first_match_idx = min(first_match_idx, m.start())

    year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', filename)
    if year_match:
        first_match_idx = min(first_match_idx, year_match.start())
        
    return filename[:first_match_idx].strip()

_QUALITY_CUT = re.compile(
    r'[\[(]|\b(?:2160p|1080p|720p|480p|360p|4k|web[- ]?dl|web[- ]?rip|blu[- ]?ray|brrip|hdrip|hevc|x26[45]|h\.?26[45]|dual|multi)\b',
    re.IGNORECASE,
)

def extract_show_title(filename: str) -> str:
    """
    Best-effort show title from an episode filename.
    Handles tags placed BEFORE the title, e.g. "[S3-22] Attack On Titan [1080p] [Dual] X.mkv"
    as well as the usual "Show.Name.S01E02.1080p.mkv".
    """
    if not filename:
        return ""
    name = re.sub(r'\.(mkv|mp4|avi|mov|wmv|flv|webm|ts|m4v)$', '', filename, flags=re.IGNORECASE)
    prefix = _clean_title_prefix(name)
    cleaned_prefix = re.sub(r'[\s\-\u2013\u2014.,_\[\](){}]+', '', prefix)
    if cleaned_prefix:
        return prefix

    # Season/episode tag is at the very start: the title follows it.
    best = None
    for pat in _SEASON_EPISODE_PATTERNS + _EPISODE_SEASON_PATTERNS + [_SEASON_DASH_EPISODE_PATTERN, _LEADING_EPISODE_PATTERN] + _STANDALONE_EPISODE_PATTERNS:
        m = pat.search(name)
        if m and (best is None or m.start() < best.start()):
            best = m
    if not best:
        return name
    rest = name[best.end():]
    rest = re.sub(r'^[\s\]\)\-\u2013\u2014.:_]+', '', rest)
    cut = _QUALITY_CUT.search(rest)
    if cut:
        rest = rest[:cut.start()]
    return rest.strip()

def matches_title(filename: str, title: str) -> bool:
    if not title:
        return True
        
    norm_title = normalize_title(title)
    prefix = _clean_title_prefix(filename)
    norm_prefix = normalize_title(prefix) or normalize_title(filename)
    
    if norm_title in norm_prefix:
        return True
        
    words = [w for w in norm_title.split() if w not in ('a', 'an', 'the', 'and', 'or', 'of', 'in', 'to', 'for', 'with')]
    if not words:
        words = norm_title.split()
        
    return all(word in norm_prefix for word in words)



