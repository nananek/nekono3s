"""
SigV4 認証の単体テスト。
ストレージのモックは不要 — バケット一覧 (GET /) で認証のみ検証する。
"""

import hashlib
import hmac
import os
from datetime import datetime, timezone
from urllib.parse import urlencode

import pytest

from tests.conftest import V4Signer


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _auth_header(
    signer: V4Signer,
    method: str = "GET",
    path: str = "/",
    body: bytes = b"",
) -> dict:
    return signer.sign(method, f"http://testserver{path}", body)


# ---------------------------------------------------------------------------
# 正常系: 有効な SigV4
# ---------------------------------------------------------------------------

def test_valid_sigv4_passes(s3):
    """正しい署名のリクエストが 200 を返す。"""
    r = s3.get("/")
    assert r.status_code == 200


def test_valid_sigv4_put_passes(s3):
    """PUT でも正しい署名なら 200。"""
    s3.put("/authbucket")          # バケット作成
    r = s3.put("/authbucket/key", body=b"hello")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# 異常系: 署名改ざん
# ---------------------------------------------------------------------------

def test_wrong_signature_rejected(client, signer):
    """署名の最後の1文字を変えると 403。"""
    headers = _auth_header(signer)
    sig_field = headers["Authorization"]
    # 末尾1文字を反転
    last = sig_field[-1]
    tampered = sig_field[:-1] + ("0" if last != "0" else "1")
    headers["Authorization"] = tampered
    r = client.get("/", headers=headers)
    assert r.status_code == 403


def test_wrong_access_key_rejected(client):
    """存在しない AccessKey は 403。"""
    bad_signer = V4Signer(access_key="wrongkey", secret_key="testsecret")
    headers = _auth_header(bad_signer)
    r = client.get("/", headers=headers)
    assert r.status_code == 403


def test_wrong_secret_rejected(client):
    """AccessKey が一致しても SecretKey が違えば署名が合わず 403。"""
    bad_signer = V4Signer(access_key="testkey", secret_key="wrongsecret")
    headers = _auth_header(bad_signer)
    r = client.get("/", headers=headers)
    assert r.status_code == 403


def test_missing_auth_header_rejected(client):
    """Authorization ヘッダーなしで 403。"""
    r = client.get("/")
    assert r.status_code == 403


def test_malformed_auth_header_rejected(client):
    """不正な Authorization ヘッダーで 400 か 403。"""
    r = client.get("/", headers={"Authorization": "AWS4-HMAC-SHA256 garbage"})
    assert r.status_code in (400, 403)


# ---------------------------------------------------------------------------
# ペイロードハッシュ
# ---------------------------------------------------------------------------

def test_unsigned_payload_accepted(client, signer):
    """x-amz-content-sha256: UNSIGNED-PAYLOAD を含むリクエストが通る。"""
    headers = signer.sign("GET", "http://testserver/")
    headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD"
    # UNSIGNED-PAYLOAD で再署名が必要なので signer.sign で extra_headers に渡す
    headers2 = signer.sign(
        "GET", "http://testserver/",
        extra_headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
    )
    r = client.get("/", headers=headers2)
    assert r.status_code == 200


def test_streaming_payload_hash_accepted(client, signer):
    """STREAMING-AWS4-HMAC-SHA256-PAYLOAD を x-amz-content-sha256 に指定できる。"""
    streaming_hash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    headers = signer.sign(
        "GET", "http://testserver/",
        extra_headers={"x-amz-content-sha256": streaming_hash},
    )
    r = client.get("/", headers=headers)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Presigned URL
# ---------------------------------------------------------------------------

def test_presigned_url_valid(client, signer, tmp_path, monkeypatch):
    """有効な Presigned URL でオブジェクト取得できる。"""
    import app.main as m
    from app.storage import FilesystemStorage

    data = str(tmp_path / "data")
    monkeypatch.setattr(m, "storage", FilesystemStorage(data))

    # オブジェクトを先に作成
    st = FilesystemStorage(data)
    st.create_bucket("presignbucket")
    from app.storage import ObjectMetadata
    st.put_object("presignbucket", "file.txt", b"hello", ObjectMetadata())

    # Presigned URL 生成
    presigned_path = signer.presign("GET", "http://testserver/presignbucket/file.txt")
    r = client.get(presigned_path)
    assert r.status_code == 200
    assert r.content == b"hello"


def test_presigned_url_tampered_rejected(client, signer, tmp_path, monkeypatch):
    """Presigned URL の X-Amz-Signature を改ざんすると 403。"""
    import app.main as m
    from app.storage import FilesystemStorage

    data = str(tmp_path / "data")
    monkeypatch.setattr(m, "storage", FilesystemStorage(data))

    presigned_path = signer.presign("GET", "http://testserver/x/y.txt")
    tampered = presigned_path[:-1] + ("0" if presigned_path[-1] != "0" else "1")
    r = client.get(tampered)
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# SigV2 (レガシー) — アクセスキー一致のみ検証
# ---------------------------------------------------------------------------

def test_sigv2_valid_key_passes(client):
    """SigV2 形式で正しい AccessKey なら通過する。"""
    r = client.get("/", headers={"Authorization": "AWS testkey:dummysig"})
    assert r.status_code == 200


def test_sigv2_wrong_key_rejected(client):
    """SigV2 形式で間違った AccessKey は 403。"""
    r = client.get("/", headers={"Authorization": "AWS wrongkey:dummysig"})
    assert r.status_code == 403
