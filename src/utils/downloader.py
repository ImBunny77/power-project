"""Robust file downloader with retry/backoff, ETag support, and content hashing."""
from __future__ import annotations

import hashlib
import io
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Default headers to look like a reasonable browser/bot
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

CACHE_DIR = Path("data/download_cache")


@dataclass
class DownloadResult:
    url: str
    content: Optional[bytes] = None
    content_hash: Optional[str] = None
    bytes_downloaded: int = 0
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    from_cache: bool = False
    unchanged: bool = False   # True when ETag/hash says content didn't change
    error: Optional[str] = None
    success: bool = False
    cached_path: Optional[Path] = None

    @property
    def text(self) -> Optional[str]:
        if self.content:
            return self.content.decode("utf-8", errors="replace")
        return None

    @property
    def as_file_like(self) -> Optional[io.BytesIO]:
        if self.content:
            return io.BytesIO(self.content)
        return None


def _make_session(retries: int = 1, backoff_factor: float = 0.3) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(DEFAULT_HEADERS)
    return session


def _get_cache_path(url: str, suffix: str = "") -> Path:
    """Deterministic cache file path based on URL hash."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    parsed = urlparse(url)
    ext = Path(parsed.path).suffix or ".bin"
    return CACHE_DIR / f"{url_hash}{ext}"


def _load_etag_cache() -> dict:
    """Load ETag/Last-Modified cache from disk."""
    etag_file = CACHE_DIR / "_etag_cache.json"
    if etag_file.exists():
        import json
        try:
            return json.loads(etag_file.read_text())
        except Exception:
            return {}
    return {}


def _save_etag_cache(cache: dict):
    import json
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    etag_file = CACHE_DIR / "_etag_cache.json"
    etag_file.write_text(json.dumps(cache, indent=2))


def download_file(
    url: str,
    timeout: int = 20,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_size_mb: int = 100,
    extra_headers: Optional[dict] = None,
) -> DownloadResult:
    """
    Download a file with retry/backoff and ETag/content-hash caching.

    Returns DownloadResult with content or error.
    """
    session = _make_session()
    cache_path = _get_cache_path(url)
    etag_store = _load_etag_cache() if use_cache else {}
    cached_meta = etag_store.get(url, {})

    headers = {}
    if extra_headers:
        headers.update(extra_headers)

    # Conditional request headers
    if not force_refresh and use_cache:
        if cached_meta.get("etag"):
            headers["If-None-Match"] = cached_meta["etag"]
        elif cached_meta.get("last_modified"):
            headers["If-Modified-Since"] = cached_meta["last_modified"]

    try:
        logger.info(f"Downloading: {url}")
        resp = session.get(url, headers=headers, timeout=timeout, stream=True)

        # 304 Not Modified — use cached content
        if resp.status_code == 304 and cache_path.exists():
            logger.info(f"Not modified (304): {url}")
            content = cache_path.read_bytes()
            return DownloadResult(
                url=url,
                content=content,
                content_hash=hashlib.md5(content).hexdigest(),
                bytes_downloaded=0,
                status_code=304,
                unchanged=True,
                from_cache=True,
                success=True,
                cached_path=cache_path,
            )

        if resp.status_code != 200:
            return DownloadResult(
                url=url,
                status_code=resp.status_code,
                error=f"HTTP {resp.status_code}: {resp.reason}",
                success=False,
            )

        # Check size before downloading
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            return DownloadResult(
                url=url,
                status_code=resp.status_code,
                error=f"File too large: {int(content_length) / 1024 / 1024:.1f} MB > {max_size_mb} MB limit",
                success=False,
            )

        # Stream content
        chunks = []
        total = 0
        for chunk in resp.iter_content(chunk_size=65536):
            chunks.append(chunk)
            total += len(chunk)
            if total > max_size_mb * 1024 * 1024:
                return DownloadResult(
                    url=url,
                    error=f"File exceeded {max_size_mb} MB during download",
                    success=False,
                )
        content = b"".join(chunks)
        content_hash = hashlib.md5(content).hexdigest()

        # Check if content actually changed
        if use_cache and cache_path.exists() and not force_refresh:
            old_hash = cached_meta.get("content_hash")
            if old_hash and old_hash == content_hash:
                logger.info(f"Content unchanged (hash match): {url}")
                return DownloadResult(
                    url=url,
                    content=content,
                    content_hash=content_hash,
                    bytes_downloaded=0,
                    status_code=200,
                    unchanged=True,
                    from_cache=True,
                    success=True,
                    cached_path=cache_path,
                )

        # Save to cache
        if use_cache:
            cache_path.write_bytes(content)
            etag_store[url] = {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "content_hash": content_hash,
            }
            _save_etag_cache(etag_store)

        return DownloadResult(
            url=url,
            content=content,
            content_hash=content_hash,
            bytes_downloaded=total,
            status_code=200,
            content_type=resp.headers.get("Content-Type"),
            etag=resp.headers.get("ETag"),
            last_modified=resp.headers.get("Last-Modified"),
            success=True,
            cached_path=cache_path,
        )

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout downloading: {url}")
        # Try cache fallback
        if cache_path.exists():
            content = cache_path.read_bytes()
            return DownloadResult(
                url=url, content=content,
                content_hash=hashlib.md5(content).hexdigest(),
                from_cache=True, success=True, error="timeout_using_cache",
                cached_path=cache_path,
            )
        return DownloadResult(url=url, error="Request timed out", success=False)

    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error downloading {url}: {e}")
        if cache_path.exists():
            content = cache_path.read_bytes()
            return DownloadResult(
                url=url, content=content,
                content_hash=hashlib.md5(content).hexdigest(),
                from_cache=True, success=True, error=f"connection_error_using_cache: {e}",
                cached_path=cache_path,
            )
        return DownloadResult(url=url, error=f"Connection error: {e}", success=False)

    except Exception as e:
        logger.exception(f"Unexpected error downloading {url}: {e}")
        return DownloadResult(url=url, error=str(e), success=False)


def download_with_fallback(urls: list[str], **kwargs) -> DownloadResult:
    """Try multiple URLs in order, returning first success."""
    for url in urls:
        result = download_file(url, **kwargs)
        if result.success:
            return result
    # Return last failure
    return result  # type: ignore
