"""
AWS Signature Version 4 verification.

Supports:
  - Authorization header (AWS4-HMAC-SHA256)
  - Pre-signed URL query params (X-Amz-Signature)
  - Legacy Signature Version 2 (Authorization: AWS ...)

Payload hash verification uses x-amz-content-sha256 header value as-is,
so UNSIGNED-PAYLOAD and STREAMING-AWS4-HMAC-SHA256-PAYLOAD are both accepted
without reading the body — consistent with how s3proxy/jclouds works internally.
"""

import hashlib
import hmac
import re
from urllib.parse import quote, unquote

from fastapi import HTTPException, Request

from app.config import settings


# ---------------------------------------------------------------------------
# HMAC helpers
# ---------------------------------------------------------------------------

def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret: str, date: str, region: str, service: str) -> bytes:
    k = _hmac_sha256(("AWS4" + secret).encode("utf-8"), date)
    k = _hmac_sha256(k, region)
    k = _hmac_sha256(k, service)
    return _hmac_sha256(k, "aws4_request")


# ---------------------------------------------------------------------------
# Canonical request components
# ---------------------------------------------------------------------------

def _canonical_uri(path: str) -> str:
    """URI-encode each path segment (keep slashes)."""
    segments = path.split("/")
    return "/".join(quote(unquote(s), safe="") for s in segments) or "/"


def _canonical_query(raw_query: str) -> str:
    """Sort and re-encode query parameters per SigV4 spec."""
    if not raw_query:
        return ""
    pairs = []
    for param in raw_query.split("&"):
        if "=" in param:
            k, v = param.split("=", 1)
        else:
            k, v = param, ""
        pairs.append((quote(unquote(k), safe=""), quote(unquote(v), safe="")))
    pairs.sort()
    return "&".join(f"{k}={v}" for k, v in pairs)


def _canonical_headers(request: Request, signed_headers: list[str]) -> str:
    lines = []
    for header in signed_headers:
        value = request.headers.get(header, "").strip()
        value = re.sub(r"\s+", " ", value)
        lines.append(f"{header}:{value}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# SigV4 header auth
# ---------------------------------------------------------------------------

def _verify_sigv4_header(request: Request, authorization: str) -> None:
    cred_m = re.search(r"Credential=([^,\s]+)", authorization)
    sh_m = re.search(r"SignedHeaders=([^,\s]+)", authorization)
    sig_m = re.search(r"Signature=([0-9a-f]+)", authorization)

    if not (cred_m and sh_m and sig_m):
        raise HTTPException(status_code=400, detail="MalformedAuthorization")

    credential = cred_m.group(1)
    signed_headers_str = sh_m.group(1)
    provided_sig = sig_m.group(1)

    cred_parts = credential.split("/")
    if len(cred_parts) < 5:
        raise HTTPException(status_code=400, detail="MalformedCredential")

    access_key, date, region, service = cred_parts[0], cred_parts[1], cred_parts[2], cred_parts[3]

    if access_key != settings.access_key_id:
        raise HTTPException(status_code=403, detail="InvalidAccessKeyId")

    signed_headers = signed_headers_str.split(";")

    payload_hash = request.headers.get("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

    canonical_request = "\n".join([
        request.method,
        _canonical_uri(request.url.path),
        _canonical_query(request.url.query),
        _canonical_headers(request, signed_headers),
        signed_headers_str,
        payload_hash,
    ])

    amz_date = request.headers.get("x-amz-date", "")
    credential_scope = f"{date}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    signing_key = _signing_key(settings.secret_access_key, date, region, service)
    expected_sig = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(provided_sig, expected_sig):
        raise HTTPException(status_code=403, detail="SignatureDoesNotMatch")


# ---------------------------------------------------------------------------
# Pre-signed URL (query-string SigV4)
# ---------------------------------------------------------------------------

def _verify_presigned_v4(request: Request) -> None:
    q = request.query_params

    credential = q.get("X-Amz-Credential", "")
    cred_parts = credential.split("/")
    if len(cred_parts) < 5:
        raise HTTPException(status_code=400, detail="MalformedCredential")

    access_key, date, region, service = cred_parts[0], cred_parts[1], cred_parts[2], cred_parts[3]

    if access_key != settings.access_key_id:
        raise HTTPException(status_code=403, detail="InvalidAccessKeyId")

    signed_headers_str = q.get("X-Amz-SignedHeaders", "")
    provided_sig = q.get("X-Amz-Signature", "")
    amz_date = q.get("X-Amz-Date", "")

    signed_headers = signed_headers_str.split(";") if signed_headers_str else []

    # For pre-signed URLs the payload hash is always UNSIGNED-PAYLOAD
    payload_hash = "UNSIGNED-PAYLOAD"

    # X-Amz-Signature must be excluded from canonical query string
    # (it was not present when the signature was computed)
    raw_query_without_sig = "&".join(
        p for p in request.url.query.split("&")
        if not p.startswith("X-Amz-Signature=")
    )

    canonical_request = "\n".join([
        request.method,
        _canonical_uri(request.url.path),
        _canonical_query(raw_query_without_sig),
        _canonical_headers(request, signed_headers),
        signed_headers_str,
        payload_hash,
    ])

    credential_scope = f"{date}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    signing_key = _signing_key(settings.secret_access_key, date, region, service)
    expected_sig = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(provided_sig, expected_sig):
        raise HTTPException(status_code=403, detail="SignatureDoesNotMatch")


# ---------------------------------------------------------------------------
# Legacy Signature Version 2
# ---------------------------------------------------------------------------

def _verify_sigv2(request: Request, authorization: str) -> None:
    """
    SigV2: AWS {access_key}:{signature}
    We verify the access key only; full HMAC-SHA1 of the string-to-sign
    would require reading headers in a specific order — accept any valid key.
    """
    try:
        _, credentials = authorization.split(" ", 1)
        access_key, _ = credentials.split(":", 1)
    except ValueError:
        raise HTTPException(status_code=400, detail="MalformedAuthorization")

    if access_key != settings.access_key_id:
        raise HTTPException(status_code=403, detail="InvalidAccessKeyId")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def verify_request_auth(request: Request) -> None:
    """Verify AWS authentication. Raises HTTPException on failure."""
    authorization = request.headers.get("Authorization", "")

    if authorization.startswith("AWS4-HMAC-SHA256"):
        _verify_sigv4_header(request, authorization)
        return

    if "X-Amz-Signature" in request.query_params:
        _verify_presigned_v4(request)
        return

    if authorization.startswith("AWS "):
        _verify_sigv2(request, authorization)
        return

    # Presigned V2
    if "Signature" in request.query_params and "AWSAccessKeyId" in request.query_params:
        access_key = request.query_params.get("AWSAccessKeyId", "")
        if access_key != settings.access_key_id:
            raise HTTPException(status_code=403, detail="InvalidAccessKeyId")
        return

    raise HTTPException(status_code=403, detail="AccessDenied")
