"""
pytest fixtures と SigV4 署名ヘルパー。

conftest.py は pytest が最初にロードするため、
ここで環境変数を設定すれば pydantic-settings の読み込みより先に適用される。
"""

import hashlib
import hmac
import os
from datetime import datetime, timezone
from urllib.parse import quote, unquote, urlencode, urlparse

# ---- app モジュールが import される前に設定 ----
os.environ["S3_ACCESS_KEY_ID"] = "testkey"
os.environ["S3_SECRET_ACCESS_KEY"] = "testsecret"
os.environ["S3_REGION"] = "us-east-1"
# モジュールロード時の初期化用 (各テストは client fixture でパッチする)
os.environ.setdefault("S3_STORAGE_PATH", "/tmp/_nekono3s_test_init")

import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.main import app
from app.multipart import MultipartManager
from app.storage import FilesystemStorage, XATTR_PREFIX_JCLOUDS


# ---------------------------------------------------------------------------
# SigV4 署名ヘルパー
# ---------------------------------------------------------------------------

class V4Signer:
    """テスト用 AWS Signature Version 4 署名クラス。"""

    def __init__(
        self,
        access_key: str = "testkey",
        secret_key: str = "testsecret",
        region: str = "us-east-1",
        service: str = "s3",
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def _hmac(self, key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _signing_key(self, date: str) -> bytes:
        k = self._hmac(f"AWS4{self.secret_key}".encode(), date)
        k = self._hmac(k, self.region)
        k = self._hmac(k, self.service)
        return self._hmac(k, "aws4_request")

    def _canonical_uri(self, path: str) -> str:
        return "/".join(quote(unquote(s), safe="") for s in path.split("/")) or "/"

    def _canonical_query(self, raw: str) -> str:
        if not raw:
            return ""
        pairs = []
        for p in raw.split("&"):
            k, _, v = p.partition("=")
            pairs.append((quote(unquote(k), safe=""), quote(unquote(v), safe="")))
        pairs.sort()
        return "&".join(f"{k}={v}" for k, v in pairs)

    def sign(
        self,
        method: str,
        url: str,
        body: bytes = b"",
        extra_headers: dict | None = None,
    ) -> dict:
        """Authorization ヘッダーを含むヘッダー辞書を返す。"""
        parsed = urlparse(url)
        host = parsed.netloc
        path = parsed.path or "/"
        query = parsed.query or ""

        now = datetime.now(tz=timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date = now.strftime("%Y%m%d")

        base_headers: dict[str, str] = {
            "host": host,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": hashlib.sha256(body).hexdigest(),
        }
        if extra_headers:
            base_headers.update({k.lower(): v for k, v in extra_headers.items()})

        # extra_headers が x-amz-content-sha256 を上書きした場合もその値を使う
        effective_payload_hash = base_headers["x-amz-content-sha256"]

        signed_list = sorted(base_headers)
        signed_str = ";".join(signed_list)
        canonical_headers = "".join(f"{k}:{base_headers[k]}\n" for k in signed_list)

        canonical_request = "\n".join([
            method.upper(),
            self._canonical_uri(path),
            self._canonical_query(query),
            canonical_headers,
            signed_str,
            effective_payload_hash,
        ])

        credential_scope = f"{date}/{self.region}/{self.service}/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ])

        sig = hmac.new(
            self._signing_key(date), string_to_sign.encode(), hashlib.sha256
        ).hexdigest()

        headers: dict[str, str] = {
            "Authorization": (
                f"AWS4-HMAC-SHA256 "
                f"Credential={self.access_key}/{credential_scope}, "
                f"SignedHeaders={signed_str}, "
                f"Signature={sig}"
            ),
            "x-amz-date": amz_date,
            "x-amz-content-sha256": effective_payload_hash,
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def presign(self, method: str, url: str, expires_in: int = 3600) -> str:
        """Presigned URL (パス + クエリ文字列) を返す。"""
        parsed = urlparse(url)
        host = parsed.netloc
        path = parsed.path or "/"

        now = datetime.now(tz=timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date = now.strftime("%Y%m%d")
        credential_scope = f"{date}/{self.region}/{self.service}/aws4_request"

        params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{self.access_key}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires_in),
            "X-Amz-SignedHeaders": "host",
        }

        canonical_qs = self._canonical_query(urlencode(params))
        canonical_request = "\n".join([
            method.upper(),
            self._canonical_uri(path),
            canonical_qs,
            f"host:{host}\n",
            "host",
            "UNSIGNED-PAYLOAD",
        ])

        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ])

        sig = hmac.new(
            self._signing_key(date), string_to_sign.encode(), hashlib.sha256
        ).hexdigest()

        params["X-Amz-Signature"] = sig
        return path + "?" + urlencode(params)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def storage(tmp_path):
    return FilesystemStorage(str(tmp_path / "data"))


@pytest.fixture()
def storage_jclouds(tmp_path):
    return FilesystemStorage(str(tmp_path / "data"), xattr_prefix=XATTR_PREFIX_JCLOUDS)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """パッチ済みストレージを持つ TestClient。"""
    data = str(tmp_path / "data")
    monkeypatch.setattr(main_module, "storage", FilesystemStorage(data))
    monkeypatch.setattr(main_module, "multipart", MultipartManager(data + "/.multipart"))
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def signer():
    return V4Signer()


class _S3Client:
    """署名付きリクエストを発行するラッパー。"""

    def __init__(self, client: TestClient, signer: V4Signer, base: str = "http://testserver"):
        self._c = client
        self._s = signer
        self._base = base

    def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        params: dict | None = None,
        extra_headers: dict | None = None,
    ):
        qs = ("?" + urlencode(params)) if params else ""
        url = self._base + path + qs
        headers = self._s.sign(method, url, body, extra_headers)
        return self._c.request(
            method, path, content=body, headers=headers, params=params
        )

    def put(self, path, body=b"", **kw):
        return self.request("PUT", path, body, **kw)

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def head(self, path, **kw):
        return self.request("HEAD", path, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)

    def post(self, path, body=b"", **kw):
        return self.request("POST", path, body, **kw)


@pytest.fixture()
def s3(client, signer):
    return _S3Client(client, signer)
