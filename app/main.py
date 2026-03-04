"""
FastAPI S3-compatible object storage.

Supported operations:
  Service:    GET /                           List buckets
  Bucket:     PUT /{bucket}                   Create bucket
              HEAD /{bucket}                  Check bucket
              DELETE /{bucket}                Delete (empty) bucket
              GET /{bucket}[?prefix&delimiter&max-keys&marker]  List objects
              POST /{bucket}?delete           Batch delete objects
              GET /{bucket}?acl               Stub ACL
              GET /{bucket}?location          Bucket location
              GET /{bucket}?uploads           List multipart uploads (stub)
  Object:     PUT /{bucket}/{key}             Put object
              GET /{bucket}/{key}             Get object (with Range support)
              HEAD /{bucket}/{key}            Head object
              DELETE /{bucket}/{key}          Delete object
  Multipart:  POST /{bucket}/{key}?uploads    Initiate multipart upload
              PUT /{bucket}/{key}?partNumber=N&uploadId=X  Upload part
              POST /{bucket}/{key}?uploadId=X Complete multipart upload
              DELETE /{bucket}/{key}?uploadId=X  Abort multipart upload
              GET /{bucket}/{key}?uploadId=X  List parts
"""

import base64
import hashlib
import re
import uuid
from datetime import datetime, timezone
from typing import Optional
from xml.etree import ElementTree as ET

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from app.auth import verify_request_auth
from app.config import settings
from app.multipart import MultipartManager
from app.storage import (
    FilesystemStorage,
    ObjectMetadata,
    XATTR_PREFIX_JCLOUDS,
    XATTR_PREFIX_NATIVE,
)
import app.xml_utils as xml

app = FastAPI(title="s3-compat")

_xattr_prefix = XATTR_PREFIX_JCLOUDS if settings.xattr_jclouds_compat else XATTR_PREFIX_NATIVE
storage = FilesystemStorage(settings.storage_path, xattr_prefix=_xattr_prefix)
multipart = MultipartManager(settings.storage_path + "/.multipart")

_REQUEST_ID = "0000000000000000"
_XML_CT = "application/xml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _xml_resp(content: bytes, status: int = 200, extra: dict | None = None) -> Response:
    headers = {"x-amz-request-id": _REQUEST_ID, "x-amz-id-2": _REQUEST_ID}
    if extra:
        headers.update(extra)
    return Response(content=content, status_code=status,
                    headers=headers, media_type=_XML_CT)


def _err(code: str, message: str, status: int, resource: str = "") -> Response:
    return _xml_resp(xml.error_response(code, message, resource), status)


def _object_response_headers(meta: ObjectMetadata, size: int, etag: str) -> dict:
    h = {
        "ETag": f'"{etag}"',
        "Content-Length": str(size),
        "x-amz-request-id": _REQUEST_ID,
    }
    if meta.content_type:
        h["Content-Type"] = meta.content_type
    if meta.content_disposition:
        h["Content-Disposition"] = meta.content_disposition
    if meta.content_encoding:
        h["Content-Encoding"] = meta.content_encoding
    if meta.content_language:
        h["Content-Language"] = meta.content_language
    if meta.expires:
        h["Expires"] = meta.expires
    if meta.cache_control:
        h["Cache-Control"] = meta.cache_control
    if meta.last_modified:
        h["Last-Modified"] = meta.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")
    for k, v in meta.user_metadata.items():
        h[f"x-amz-meta-{k}"] = v
    return h


def _parse_user_metadata(request: Request) -> dict[str, str]:
    """Extract x-amz-meta-* headers. Strip prefix before storing (jclouds compat)."""
    meta = {}
    for k, v in request.headers.items():
        if k.lower().startswith("x-amz-meta-"):
            meta[k.lower()[len("x-amz-meta-"):]] = v
    return meta


async def _decode_request_stream(request: Request):
    """
    Yield raw body chunks.
    Handles aws-chunked (STREAMING-AWS4-HMAC-SHA256-PAYLOAD) transparently.
    """
    content_encoding = request.headers.get("content-encoding", "")
    sha256_header = request.headers.get("x-amz-content-sha256", "")

    if "aws-chunked" in content_encoding or sha256_header.startswith("STREAMING-"):
        # AWS chunked: each chunk is prefixed by  "{size_hex};chunk-signature={sig}\r\n"
        buf = bytearray()
        async for incoming in request.stream():
            buf.extend(incoming)

        while buf:
            eol = buf.find(b"\r\n")
            if eol == -1:
                break
            header = buf[:eol].decode("ascii", errors="replace")
            chunk_size = int(header.split(";")[0], 16)
            if chunk_size == 0:
                break
            data_start = eol + 2
            data_end = data_start + chunk_size
            if len(buf) < data_end + 2:
                break
            yield bytes(buf[data_start:data_end])
            del buf[: data_end + 2]   # skip trailing \r\n
    else:
        async for chunk in request.stream():
            yield chunk


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    try:
        await verify_request_auth(request)
    except HTTPException as exc:
        return _err("AccessDenied", str(exc.detail), exc.status_code)
    return await call_next(request)


# ---------------------------------------------------------------------------
# Service: GET /  →  List buckets
# ---------------------------------------------------------------------------

@app.get("/")
async def list_buckets():
    buckets = storage.list_buckets()
    return _xml_resp(xml.list_all_my_buckets_result(
        owner_id=settings.access_key_id,
        owner_name=settings.access_key_id,
        buckets=buckets,
    ))


# ---------------------------------------------------------------------------
# Catch-all S3 router
# ---------------------------------------------------------------------------

@app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE"])
async def s3_router(path: str, request: Request):
    parts = path.strip("/").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    q = request.query_params
    method = request.method

    if not bucket:
        return _err("InvalidRequest", "Missing bucket", 400)

    # ----------------------------------------------------------------
    # Bucket-level operations (no key)
    # ----------------------------------------------------------------
    if not key:
        if method == "HEAD":
            return _handle_head_bucket(bucket)
        if method == "PUT":
            return _handle_create_bucket(bucket, request)
        if method == "DELETE":
            return _handle_delete_bucket(bucket)
        if method == "GET":
            if "acl" in q:
                return _stub_acl(bucket)
            if "location" in q:
                return _handle_bucket_location(bucket)
            if "uploads" in q:
                return _xml_resp(xml.list_multipart_uploads_result(bucket))
            return _handle_list_objects(bucket, request)
        if method == "POST":
            if "delete" in q:
                return await _handle_delete_objects(bucket, request)
        return _err("NotImplemented", "Operation not supported", 501)

    # ----------------------------------------------------------------
    # Object-level operations
    # ----------------------------------------------------------------
    if method == "HEAD":
        return _handle_head_object(bucket, key)

    if method == "GET":
        if "uploadId" in q:
            return _handle_list_parts(bucket, key, q["uploadId"])
        return _handle_get_object(bucket, key, request)

    if method == "PUT":
        if "partNumber" in q and "uploadId" in q:
            return await _handle_upload_part(bucket, key, request,
                                              int(q["partNumber"]), q["uploadId"])
        return await _handle_put_object(bucket, key, request)

    if method == "POST":
        if "uploads" in q:
            return _handle_initiate_multipart(bucket, key, request)
        if "uploadId" in q:
            return await _handle_complete_multipart(bucket, key, request, q["uploadId"])

    if method == "DELETE":
        if "uploadId" in q:
            return _handle_abort_multipart(q["uploadId"])
        return _handle_delete_object(bucket, key)

    return _err("NotImplemented", "Operation not supported", 501)


# ---------------------------------------------------------------------------
# Bucket handlers
# ---------------------------------------------------------------------------

def _handle_head_bucket(bucket: str) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")
    return Response(status_code=200, headers={"x-amz-request-id": _REQUEST_ID})


def _handle_create_bucket(bucket: str, request: Request) -> Response:
    storage.create_bucket(bucket)
    return Response(
        status_code=200,
        headers={"Location": f"/{bucket}", "x-amz-request-id": _REQUEST_ID},
    )


def _handle_delete_bucket(bucket: str) -> Response:
    try:
        storage.delete_bucket(bucket)
    except FileNotFoundError:
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")
    except ValueError:
        return _err("BucketNotEmpty", "The bucket you tried to delete is not empty.", 409,
                    f"/{bucket}")
    return Response(status_code=204, headers={"x-amz-request-id": _REQUEST_ID})


def _handle_list_objects(bucket: str, request: Request) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")
    q = request.query_params
    try:
        result = storage.list_objects(
            bucket,
            prefix=q.get("prefix", ""),
            delimiter=q.get("delimiter", ""),
            max_keys=int(q.get("max-keys", 1000)),
            marker=q.get("marker", ""),
        )
    except FileNotFoundError:
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")

    return _xml_resp(xml.list_bucket_result(
        bucket=bucket,
        prefix=q.get("prefix", ""),
        delimiter=q.get("delimiter", ""),
        max_keys=int(q.get("max-keys", 1000)),
        objects=result["objects"],
        common_prefixes=result["common_prefixes"],
        is_truncated=result["is_truncated"],
        marker=q.get("marker", ""),
        next_marker=result["next_marker"],
    ))


def _stub_acl(bucket: str) -> Response:
    """Return a minimal private ACL."""
    body = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b'<Owner><ID>owner</ID><DisplayName>owner</DisplayName></Owner>'
        b'<AccessControlList>'
        b'<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        b'xsi:type="CanonicalUser"><ID>owner</ID><DisplayName>owner</DisplayName>'
        b'</Grantee><Permission>FULL_CONTROL</Permission></Grant>'
        b'</AccessControlList></AccessControlPolicy>'
    )
    return _xml_resp(body)


def _handle_bucket_location(bucket: str) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")
    body = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        + settings.region.encode()
        + b'</LocationConstraint>'
    )
    return _xml_resp(body)


async def _handle_delete_objects(bucket: str, request: Request) -> Response:
    """POST /{bucket}?delete  — batch delete"""
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")
    body = await request.body()
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return _err("MalformedXML", "Invalid XML", 400)

    deleted: list[str] = []
    errors: list[dict] = []
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    obj_els = root.findall(".//s3:Object", ns)
    if not obj_els:
        obj_els = root.findall(".//Object")
    for obj_el in obj_els:
        key_el = obj_el.find("s3:Key", ns)
        if key_el is None:
            key_el = obj_el.find("Key")
        if key_el is None or not key_el.text:
            continue
        k = key_el.text
        storage.delete_object(bucket, k)
        deleted.append(k)

    return _xml_resp(xml.delete_result(deleted, errors))


# ---------------------------------------------------------------------------
# Object handlers
# ---------------------------------------------------------------------------

async def _handle_put_object(bucket: str, key: str, request: Request) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}")

    # Parse content-md5 header (base64 encoded, optional)
    provided_md5: Optional[bytes] = None
    if cm := request.headers.get("content-md5"):
        try:
            provided_md5 = base64.b64decode(cm)
        except Exception:
            return _err("InvalidDigest", "Invalid Content-MD5", 400)

    # aws-chunked は HTTP 転送エンコーディングであり、オブジェクトの
    # Content-Encoding として保存してはならない
    raw_ce = request.headers.get("content-encoding", "")
    stored_ce = raw_ce if raw_ce and raw_ce.lower() != "aws-chunked" else None

    meta = ObjectMetadata(
        content_type=request.headers.get("content-type", "application/octet-stream"),
        content_disposition=request.headers.get("content-disposition"),
        content_encoding=stored_ce,
        content_language=request.headers.get("content-language"),
        expires=request.headers.get("expires"),
        cache_control=request.headers.get("cache-control"),
        # content_md5 は put_object_stream が実データから計算して上書きする
        user_metadata=_parse_user_metadata(request),
    )

    etag = await storage.put_object_stream(bucket, key, _decode_request_stream(request), meta)

    # Verify Content-MD5 if provided
    if provided_md5 and provided_md5 != bytes.fromhex(etag):
        storage.delete_object(bucket, key)
        return _err("BadDigest", "Content-MD5 mismatch", 400)

    return Response(
        status_code=200,
        headers={"ETag": f'"{etag}"', "x-amz-request-id": _REQUEST_ID},
    )


def _handle_head_object(bucket: str, key: str) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}/{key}")
    try:
        size, meta = storage.head_object(bucket, key)
    except FileNotFoundError:
        return _err("NoSuchKey", "The specified key does not exist.", 404, f"/{bucket}/{key}")

    headers = _object_response_headers(meta, size, meta.etag or "")
    return Response(status_code=200, headers=headers)


def _handle_get_object(bucket: str, key: str, request: Request) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}/{key}")
    try:
        obj_path = storage.get_object_path(bucket, key)
        size, meta = storage.head_object(bucket, key)
    except FileNotFoundError:
        return _err("NoSuchKey", "The specified key does not exist.", 404, f"/{bucket}/{key}")

    etag = meta.etag or ""
    headers = _object_response_headers(meta, size, etag)

    # Range request
    range_header = request.headers.get("range")
    if range_header:
        m = re.match(r"bytes=(\d*)-(\d*)", range_header)
        if m:
            start_s, end_s = m.groups()
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else size - 1
            if start > end or start >= size:
                return Response(
                    status_code=416,
                    headers={"Content-Range": f"bytes */{size}", "x-amz-request-id": _REQUEST_ID},
                )
            end = min(end, size - 1)
            length = end - start + 1
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
            headers["Content-Length"] = str(length)

            def _range_iter():
                with open(obj_path, "rb") as f:
                    f.seek(start)
                    remaining = length
                    while remaining > 0:
                        chunk = f.read(min(65536, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk

            return StreamingResponse(_range_iter(), status_code=206, headers=headers)

    def _full_iter():
        with open(obj_path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    return StreamingResponse(_full_iter(), status_code=200, headers=headers)


def _handle_delete_object(bucket: str, key: str) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}/{key}")
    storage.delete_object(bucket, key)
    return Response(status_code=204, headers={"x-amz-request-id": _REQUEST_ID})


# ---------------------------------------------------------------------------
# Multipart upload handlers
# ---------------------------------------------------------------------------

def _handle_initiate_multipart(bucket: str, key: str, request: Request) -> Response:
    if not storage.bucket_exists(bucket):
        return _err("NoSuchBucket", "The specified bucket does not exist.", 404, f"/{bucket}/{key}")
    meta = ObjectMetadata(
        content_type=request.headers.get("content-type", "application/octet-stream"),
        content_disposition=request.headers.get("content-disposition"),
        content_encoding=request.headers.get("content-encoding"),
        content_language=request.headers.get("content-language"),
        expires=request.headers.get("expires"),
        cache_control=request.headers.get("cache-control"),
        user_metadata=_parse_user_metadata(request),
    )
    upload_id = multipart.initiate(bucket, key, meta)
    return _xml_resp(xml.initiate_multipart_upload_result(bucket, key, upload_id))


async def _handle_upload_part(
    bucket: str, key: str, request: Request, part_number: int, upload_id: str
) -> Response:
    try:
        etag = await multipart.upload_part_stream(upload_id, part_number,
                                                   _decode_request_stream(request))
    except FileNotFoundError:
        return _err("NoSuchUpload", "The specified upload does not exist.", 404)
    return Response(
        status_code=200,
        headers={"ETag": etag, "x-amz-request-id": _REQUEST_ID},
    )


async def _handle_complete_multipart(
    bucket: str, key: str, request: Request, upload_id: str
) -> Response:
    body = await request.body()
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return _err("MalformedXML", "Invalid XML", 400)

    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    parts = []
    for p in root.findall(".//s3:Part", ns) or root.findall(".//Part"):
        pn_el = p.find("s3:PartNumber", ns) or p.find("PartNumber")
        if pn_el is not None and pn_el.text:
            parts.append(int(pn_el.text))
    parts.sort()

    try:
        bkt, obj_key, meta, etag_str = multipart.complete(
            upload_id, parts, str(storage.base)
        )
    except FileNotFoundError as e:
        return _err("NoSuchUpload", str(e), 404)

    # Write system metadata via storage (xattr)
    obj_path = storage.get_object_path(bkt, obj_key)
    storage._write_metadata(obj_path, meta)  # noqa: SLF001

    multipart.abort(upload_id)   # clean up temp parts

    location = f"http://localhost/{bkt}/{obj_key}"
    return _xml_resp(xml.complete_multipart_upload_result(location, bkt, obj_key, etag_str))


def _handle_abort_multipart(upload_id: str) -> Response:
    multipart.abort(upload_id)
    return Response(status_code=204, headers={"x-amz-request-id": _REQUEST_ID})


def _handle_list_parts(bucket: str, key: str, upload_id: str) -> Response:
    try:
        parts = multipart.list_parts(upload_id)
    except FileNotFoundError:
        return _err("NoSuchUpload", "The specified upload does not exist.", 404)
    return _xml_resp(xml.list_parts_result(bucket, key, upload_id, parts))
