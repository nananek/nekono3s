"""
Filesystem storage with xattr metadata.

XAttr key prefix modes:
  - Native (default):  user.content-type, user.{key}
  - jclouds compat:    user.user.content-type, user.user.{key}
    (s3proxy / jclouds filesystem-nio2 writes "user.X" which the OS stores
     as "user.user.X" in the user namespace)

Directory layout:
  {storage_path}/{bucket}/{key}
"""

import hashlib
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# XAttr key helpers
# ---------------------------------------------------------------------------

# Suffix keys (without namespace prefix)
_XATTR_SUFFIXES = {
    "content-type",
    "content-md5",
    "content-disposition",
    "content-encoding",
    "content-language",
    "expires",
    "cache-control",
}

# Default prefix (native mode)
XATTR_PREFIX_NATIVE = "user."
# jclouds compat prefix (s3proxy stores "user.X" → OS sees "user.user.X")
XATTR_PREFIX_JCLOUDS = "user.user."


def xattr_keys(prefix: str = XATTR_PREFIX_NATIVE) -> dict[str, str]:
    """Return a dict mapping suffix → full xattr key for the given prefix."""
    return {s: f"{prefix}{s}" for s in _XATTR_SUFFIXES}


# Backwards-compatible module-level constants (native prefix)
XATTR_CONTENT_TYPE = "user.content-type"
XATTR_CONTENT_MD5 = "user.content-md5"
XATTR_CONTENT_DISPOSITION = "user.content-disposition"
XATTR_CONTENT_ENCODING = "user.content-encoding"
XATTR_CONTENT_LANGUAGE = "user.content-language"
XATTR_EXPIRES = "user.expires"
XATTR_CACHE_CONTROL = "user.cache-control"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ObjectMetadata:
    content_type: Optional[str] = "application/octet-stream"
    content_md5: Optional[bytes] = None   # raw 16 bytes; etag = .hex()
    content_disposition: Optional[str] = None
    content_encoding: Optional[str] = None
    content_language: Optional[str] = None
    expires: Optional[str] = None
    cache_control: Optional[str] = None
    # Keys stored WITHOUT "x-amz-meta-" prefix (matches jclouds convention)
    user_metadata: dict[str, str] = field(default_factory=dict)
    # Filled by storage layer on read
    last_modified: Optional[datetime] = None
    size: Optional[int] = None

    @property
    def etag(self) -> Optional[str]:
        return self.content_md5.hex() if self.content_md5 else None


# ---------------------------------------------------------------------------
# Storage class
# ---------------------------------------------------------------------------

class FilesystemStorage:
    def __init__(self, base_path: str, xattr_prefix: str = XATTR_PREFIX_NATIVE) -> None:
        self.base = Path(base_path)
        self.base.mkdir(parents=True, exist_ok=True)
        self._xattr_prefix = xattr_prefix
        self._xkeys = xattr_keys(xattr_prefix)
        self._system_xattrs = set(self._xkeys.values())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _bucket_path(self, bucket: str) -> Path:
        return self.base / bucket

    def _object_path(self, bucket: str, key: str) -> Path:
        return self.base / bucket / key

    def _cleanup_empty_dirs(self, path: Path, stop_at: Path) -> None:
        while path != stop_at and path != path.parent:
            try:
                path.rmdir()
                path = path.parent
            except OSError:
                break

    def _compute_file_md5(self, path: Path) -> str:
        md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                md5.update(chunk)
        return md5.hexdigest()

    # ------------------------------------------------------------------
    # XAttr read / write
    # ------------------------------------------------------------------

    def _read_metadata(self, path: Path) -> ObjectMetadata:
        meta = ObjectMetadata(content_type=None)
        try:
            attrs = os.listxattr(str(path))
        except OSError:
            return meta

        k = self._xkeys
        for attr in attrs:
            try:
                raw = os.getxattr(str(path), attr)
            except OSError:
                continue

            if attr == k["content-type"]:
                meta.content_type = raw.decode("utf-8")
            elif attr == k["content-md5"]:
                meta.content_md5 = bytes(raw)
            elif attr == k["content-disposition"]:
                meta.content_disposition = raw.decode("utf-8")
            elif attr == k["content-encoding"]:
                meta.content_encoding = raw.decode("utf-8")
            elif attr == k["content-language"]:
                meta.content_language = raw.decode("utf-8")
            elif attr == k["expires"]:
                meta.expires = raw.decode("utf-8")
            elif attr == k["cache-control"]:
                meta.cache_control = raw.decode("utf-8")
            elif attr.startswith(self._xattr_prefix) and attr not in self._system_xattrs:
                meta.user_metadata[attr[len(self._xattr_prefix):]] = raw.decode("utf-8")

        return meta

    def _write_metadata(self, path: Path, meta: ObjectMetadata) -> None:
        p = str(path)
        k = self._xkeys

        def _set_str(xattr_name: str, value: Optional[str]) -> None:
            if value is not None:
                os.setxattr(p, xattr_name, value.encode("utf-8"))

        _set_str(k["content-type"], meta.content_type)
        if meta.content_md5 is not None:
            os.setxattr(p, k["content-md5"], meta.content_md5)
        _set_str(k["content-disposition"], meta.content_disposition)
        _set_str(k["content-encoding"], meta.content_encoding)
        _set_str(k["content-language"], meta.content_language)
        _set_str(k["expires"], meta.expires)
        _set_str(k["cache-control"], meta.cache_control)

        for key, value in meta.user_metadata.items():
            os.setxattr(p, f"{self._xattr_prefix}{key}", value.encode("utf-8"))

    # ------------------------------------------------------------------
    # Bucket operations
    # ------------------------------------------------------------------

    def list_buckets(self) -> list[dict]:
        result = []
        try:
            for p in sorted(self.base.iterdir()):
                if p.is_dir() and not p.name.startswith("."):
                    mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                    result.append({
                        "name": p.name,
                        "creation_date": mtime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    })
        except OSError:
            pass
        return result

    def bucket_exists(self, bucket: str) -> bool:
        return self._bucket_path(bucket).is_dir()

    def create_bucket(self, bucket: str) -> None:
        self._bucket_path(bucket).mkdir(parents=True, exist_ok=True)

    def delete_bucket(self, bucket: str) -> None:
        """Raise FileNotFoundError if missing, ValueError if not empty."""
        bp = self._bucket_path(bucket)
        if not bp.is_dir():
            raise FileNotFoundError(bucket)
        for p in bp.rglob("*"):
            if p.is_file():
                raise ValueError("BucketNotEmpty")
        # remove empty subdirs then bucket dir
        for p in sorted(bp.rglob("*"), reverse=True):
            if p.is_dir():
                p.rmdir()
        bp.rmdir()

    # ------------------------------------------------------------------
    # Object listing
    # ------------------------------------------------------------------

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        marker: str = "",
    ) -> dict:
        bp = self._bucket_path(bucket)
        if not bp.is_dir():
            raise FileNotFoundError(bucket)

        all_files = sorted(
            (str(p.relative_to(bp)).replace("\\", "/"), p)
            for p in bp.rglob("*")
            if p.is_file()
        )

        objects: list[dict] = []
        common_prefixes: set[str] = set()
        is_truncated = False
        next_marker = ""

        for key, p in all_files:
            if prefix and not key.startswith(prefix):
                continue
            if marker and key <= marker:
                continue

            if delimiter:
                after = key[len(prefix):]
                pos = after.find(delimiter)
                if pos != -1:
                    common_prefixes.add(prefix + after[: pos + len(delimiter)])
                    continue

            if len(objects) >= max_keys:
                is_truncated = True
                # NextMarker = 最後に含めたキー。次リクエストでそのキーの後から始まる
                next_marker = objects[-1]["key"] if objects else key
                break

            stat = p.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            meta = self._read_metadata(p)
            etag = meta.etag or self._compute_file_md5(p)

            objects.append({
                "key": key,
                "last_modified": mtime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "etag": f'"{etag}"',
                "size": stat.st_size,
                "storage_class": "STANDARD",
            })

        return {
            "objects": objects,
            "common_prefixes": sorted(common_prefixes),
            "is_truncated": is_truncated,
            "next_marker": next_marker,
        }

    # ------------------------------------------------------------------
    # Object write
    # ------------------------------------------------------------------

    async def put_object_stream(
        self, bucket: str, key: str, stream, meta: ObjectMetadata
    ) -> str:
        """Write object from async iterable. Returns hex ETag (MD5)."""
        obj_path = self._object_path(bucket, key)
        obj_path.parent.mkdir(parents=True, exist_ok=True)

        md5 = hashlib.md5()
        with open(obj_path, "wb") as f:
            async for chunk in stream:
                f.write(chunk)
                md5.update(chunk)

        # 常に実際のデータ MD5 を使う (Content-MD5 ヘッダー検証は呼び出し元で行う)
        meta.content_md5 = md5.digest()

        self._write_metadata(obj_path, meta)
        return meta.content_md5.hex()

    def put_object(self, bucket: str, key: str, data: bytes, meta: ObjectMetadata) -> str:
        """Write object from bytes. Returns hex ETag (MD5)."""
        obj_path = self._object_path(bucket, key)
        obj_path.parent.mkdir(parents=True, exist_ok=True)

        if meta.content_md5 is None:
            meta.content_md5 = hashlib.md5(data).digest()

        obj_path.write_bytes(data)
        self._write_metadata(obj_path, meta)
        return meta.content_md5.hex()

    # ------------------------------------------------------------------
    # Object read / head
    # ------------------------------------------------------------------

    def get_object_path(self, bucket: str, key: str) -> Path:
        p = self._object_path(bucket, key)
        if not p.is_file():
            raise FileNotFoundError(key)
        return p

    def head_object(self, bucket: str, key: str) -> tuple[int, ObjectMetadata]:
        p = self._object_path(bucket, key)
        if not p.is_file():
            raise FileNotFoundError(key)
        stat = p.stat()
        meta = self._read_metadata(p)
        meta.size = stat.st_size
        meta.last_modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if meta.content_md5 is None:
            meta.content_md5 = bytes.fromhex(self._compute_file_md5(p))
        return stat.st_size, meta

    # ------------------------------------------------------------------
    # Object delete
    # ------------------------------------------------------------------

    def delete_object(self, bucket: str, key: str) -> bool:
        p = self._object_path(bucket, key)
        if not p.is_file():
            return False
        p.unlink()
        self._cleanup_empty_dirs(p.parent, self._bucket_path(bucket))
        return True
