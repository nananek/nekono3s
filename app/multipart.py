"""
Multipart upload manager.

Parts are stored under:
  {storage_path}/.multipart/{upload_id}/part.{part_number:05d}

Upload metadata (bucket, key, ObjectMetadata) is stored as JSON:
  {storage_path}/.multipart/{upload_id}/meta.json

ETag for completed upload: hex(MD5(concat(part_md5_bytes))) + "-{n_parts}"
"""

import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from app.storage import ObjectMetadata


class MultipartManager:
    def __init__(self, tmp_path: str) -> None:
        self.tmp = Path(tmp_path)
        self.tmp.mkdir(parents=True, exist_ok=True)

    def _upload_dir(self, upload_id: str) -> Path:
        return self.tmp / upload_id

    def _meta_path(self, upload_id: str) -> Path:
        return self._upload_dir(upload_id) / "meta.json"

    def _part_path(self, upload_id: str, part_number: int) -> Path:
        return self._upload_dir(upload_id) / f"part.{part_number:05d}"

    # ------------------------------------------------------------------

    def initiate(self, bucket: str, key: str, meta: ObjectMetadata) -> str:
        upload_id = str(uuid.uuid4())
        d = self._upload_dir(upload_id)
        d.mkdir(parents=True, exist_ok=True)

        meta_dict = {
            "bucket": bucket,
            "key": key,
            "content_type": meta.content_type,
            "content_disposition": meta.content_disposition,
            "content_encoding": meta.content_encoding,
            "content_language": meta.content_language,
            "expires": meta.expires,
            "cache_control": meta.cache_control,
            "user_metadata": meta.user_metadata,
        }
        self._meta_path(upload_id).write_text(json.dumps(meta_dict))
        return upload_id

    def upload_part(self, upload_id: str, part_number: int, data: bytes) -> str:
        """Store part data. Returns quoted ETag."""
        if not self._upload_dir(upload_id).is_dir():
            raise FileNotFoundError(upload_id)
        part_md5 = hashlib.md5(data).hexdigest()
        self._part_path(upload_id, part_number).write_bytes(data)
        return f'"{part_md5}"'

    async def upload_part_stream(self, upload_id: str, part_number: int, stream) -> str:
        """Stream part data from async iterable. Returns quoted ETag."""
        if not self._upload_dir(upload_id).is_dir():
            raise FileNotFoundError(upload_id)
        part_path = self._part_path(upload_id, part_number)
        md5 = hashlib.md5()
        with open(part_path, "wb") as f:
            async for chunk in stream:
                f.write(chunk)
                md5.update(chunk)
        return f'"{md5.hexdigest()}"'

    def list_parts(self, upload_id: str) -> list[dict]:
        d = self._upload_dir(upload_id)
        if not d.is_dir():
            raise FileNotFoundError(upload_id)
        parts = []
        for p in sorted(d.glob("part.*")):
            part_number = int(p.name.split(".")[1])
            stat = p.stat()
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            md5 = hashlib.md5(p.read_bytes()).hexdigest()
            parts.append({
                "part_number": part_number,
                "last_modified": mtime.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "etag": f'"{md5}"',
                "size": stat.st_size,
            })
        return parts

    def complete(
        self,
        upload_id: str,
        ordered_part_numbers: list[int],
        storage_path: str,
    ) -> tuple[str, str, ObjectMetadata, str]:
        """
        Merge parts in given order.
        Returns (bucket, key, metadata) with content_md5 set to multipart ETag bytes.
        The ETag hex is: md5(concat(part_md5_raw_bytes)).hexdigest() + "-{n}"
        """
        d = self._upload_dir(upload_id)
        if not d.is_dir():
            raise FileNotFoundError(upload_id)

        meta_dict = json.loads(self._meta_path(upload_id).read_text())
        bucket = meta_dict["bucket"]
        key = meta_dict["key"]

        meta = ObjectMetadata(
            content_type=meta_dict.get("content_type"),
            content_disposition=meta_dict.get("content_disposition"),
            content_encoding=meta_dict.get("content_encoding"),
            content_language=meta_dict.get("content_language"),
            expires=meta_dict.get("expires"),
            cache_control=meta_dict.get("cache_control"),
            user_metadata=meta_dict.get("user_metadata", {}),
        )

        # Destination file
        dest = Path(storage_path) / bucket / key
        dest.parent.mkdir(parents=True, exist_ok=True)

        part_md5_concat = b""
        with open(dest, "wb") as out:
            for pn in ordered_part_numbers:
                part_path = self._part_path(upload_id, pn)
                if not part_path.is_file():
                    raise FileNotFoundError(f"Part {pn} missing for upload {upload_id}")
                data = part_path.read_bytes()
                out.write(data)
                part_md5_concat += hashlib.md5(data).digest()

        n = len(ordered_part_numbers)
        # Multipart ETag: MD5 of concatenated per-part MD5 bytes, plus "-{n}"
        multipart_etag = hashlib.md5(part_md5_concat).hexdigest() + f"-{n}"

        # For xattr content-md5, compute the actual file MD5 (jclouds compat)
        file_md5 = hashlib.md5()
        with open(dest, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                file_md5.update(chunk)
        meta.content_md5 = file_md5.digest()

        return bucket, key, meta, multipart_etag

    def abort(self, upload_id: str) -> None:
        d = self._upload_dir(upload_id)
        if d.is_dir():
            shutil.rmtree(d)

    def get_bucket_key(self, upload_id: str) -> tuple[str, str]:
        meta_dict = json.loads(self._meta_path(upload_id).read_text())
        return meta_dict["bucket"], meta_dict["key"]
