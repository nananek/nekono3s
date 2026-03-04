"""
S3 API エンドポイントの統合テスト。

s3 fixture: V4Signer で署名済みリクエストを発行する _S3Client ラッパー。
"""

import hashlib
import base64
from xml.etree import ElementTree as ET

import pytest


# ---------------------------------------------------------------------------
# バケット操作
# ---------------------------------------------------------------------------

def test_list_buckets_empty(s3):
    r = s3.get("/")
    assert r.status_code == 200
    root = ET.fromstring(r.content)
    buckets = root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Bucket")
    assert buckets == []


def test_create_bucket(s3):
    r = s3.put("/mybucket")
    assert r.status_code == 200
    assert r.headers.get("Location") == "/mybucket"


def test_create_and_list_buckets(s3):
    s3.put("/bucket1")
    s3.put("/bucket2")
    r = s3.get("/")
    assert r.status_code == 200
    names = [
        el.text
        for el in ET.fromstring(r.content).findall(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}Name"
        )
    ]
    assert "bucket1" in names
    assert "bucket2" in names


def test_head_bucket_exists(s3):
    s3.put("/hbucket")
    r = s3.head("/hbucket")
    assert r.status_code == 200


def test_head_bucket_missing(s3):
    r = s3.head("/no-such-bucket")
    assert r.status_code == 404


def test_delete_empty_bucket(s3):
    s3.put("/delbucket")
    r = s3.delete("/delbucket")
    assert r.status_code == 204
    assert s3.head("/delbucket").status_code == 404


def test_delete_nonempty_bucket_returns_409(s3):
    s3.put("/fullbucket")
    s3.put("/fullbucket/key.txt", body=b"data")
    r = s3.delete("/fullbucket")
    assert r.status_code == 409


def test_delete_missing_bucket_returns_404(s3):
    r = s3.delete("/ghost-bucket")
    assert r.status_code == 404


def test_get_bucket_location(s3):
    s3.put("/locbucket")
    r = s3.get("/locbucket", params={"location": ""})
    assert r.status_code == 200
    assert b"LocationConstraint" in r.content


def test_get_bucket_acl(s3):
    s3.put("/aclbucket")
    r = s3.get("/aclbucket", params={"acl": ""})
    assert r.status_code == 200
    assert b"FULL_CONTROL" in r.content


# ---------------------------------------------------------------------------
# オブジェクト PUT / GET / HEAD / DELETE
# ---------------------------------------------------------------------------

def test_put_and_get_object(s3):
    s3.put("/obj")
    s3.put("/obj/hello.txt", body=b"world")
    r = s3.get("/obj/hello.txt")
    assert r.status_code == 200
    assert r.content == b"world"


def test_put_returns_etag(s3):
    s3.put("/etag")
    r = s3.put("/etag/f.txt", body=b"data")
    assert r.status_code == 200
    etag = r.headers.get("ETag", "")
    expected = hashlib.md5(b"data").hexdigest()
    assert expected in etag


def test_head_object(s3):
    s3.put("/hobj")
    s3.put("/hobj/file.bin", body=b"abc",
           extra_headers={"Content-Type": "application/octet-stream"})
    r = s3.head("/hobj/file.bin")
    assert r.status_code == 200
    assert r.headers["Content-Length"] == "3"
    assert "ETag" in r.headers


def test_delete_object(s3):
    s3.put("/delobj")
    s3.put("/delobj/k.txt", body=b"x")
    r = s3.delete("/delobj/k.txt")
    assert r.status_code == 204
    assert s3.get("/delobj/k.txt").status_code == 404


def test_get_missing_object_returns_404(s3):
    s3.put("/bucket404")
    r = s3.get("/bucket404/missing.txt")
    assert r.status_code == 404


def test_get_missing_bucket_returns_404(s3):
    r = s3.get("/no-bucket/key.txt")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Content-Type / ユーザーメタデータ
# ---------------------------------------------------------------------------

def test_content_type_preserved(s3):
    s3.put("/ct")
    s3.put("/ct/img.png", body=b"\x89PNG",
           extra_headers={"Content-Type": "image/png"})
    r = s3.get("/ct/img.png")
    assert r.headers.get("Content-Type", "").startswith("image/png")


def test_user_metadata_roundtrip(s3):
    """x-amz-meta-* ヘッダーが GET レスポンスに含まれる。"""
    s3.put("/meta")
    s3.put(
        "/meta/f.txt",
        body=b"data",
        extra_headers={
            "x-amz-meta-author": "alice",
            "x-amz-meta-env": "prod",
        },
    )
    r = s3.get("/meta/f.txt")
    assert r.headers.get("x-amz-meta-author") == "alice"
    assert r.headers.get("x-amz-meta-env") == "prod"


def test_aws_chunked_encoding_not_stored_as_content_encoding(s3):
    """aws-chunked 転送エンコーディングがオブジェクトの Content-Encoding に保存されない。"""
    s3.put("/ce")
    s3.put(
        "/ce/f.txt",
        body=b"body data",
        extra_headers={"Content-Encoding": "aws-chunked"},
    )
    r = s3.head("/ce/f.txt")
    assert r.headers.get("Content-Encoding", "") != "aws-chunked"


# ---------------------------------------------------------------------------
# Content-MD5 検証
# ---------------------------------------------------------------------------

def test_put_with_valid_content_md5(s3):
    s3.put("/md5")
    data = b"checkme"
    md5_b64 = base64.b64encode(hashlib.md5(data).digest()).decode()
    r = s3.put(
        "/md5/f.txt",
        body=data,
        extra_headers={"Content-MD5": md5_b64},
    )
    assert r.status_code == 200


def test_put_with_wrong_content_md5_rejected(s3):
    s3.put("/md5bad")
    data = b"checkme"
    bad_b64 = base64.b64encode(b"\x00" * 16).decode()
    r = s3.put(
        "/md5bad/f.txt",
        body=data,
        extra_headers={"Content-MD5": bad_b64},
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Range リクエスト
# ---------------------------------------------------------------------------

def test_range_request_partial(s3):
    s3.put("/range")
    s3.put("/range/data.bin", body=b"0123456789")
    r = s3.get("/range/data.bin", extra_headers={"Range": "bytes=2-5"})
    assert r.status_code == 206
    assert r.content == b"2345"
    assert r.headers["Content-Range"] == "bytes 2-5/10"
    assert r.headers["Content-Length"] == "4"


def test_range_request_suffix(s3):
    """bytes=-N で末尾 N バイトを取得。"""
    s3.put("/range2")
    s3.put("/range2/data.bin", body=b"abcdefghij")
    r = s3.get("/range2/data.bin", extra_headers={"Range": "bytes=7-"})
    assert r.status_code == 206
    assert r.content == b"hij"


def test_range_out_of_bounds_returns_416(s3):
    s3.put("/range3")
    s3.put("/range3/small.txt", body=b"hi")
    r = s3.get("/range3/small.txt", extra_headers={"Range": "bytes=100-200"})
    assert r.status_code == 416


# ---------------------------------------------------------------------------
# オブジェクト一覧
# ---------------------------------------------------------------------------

def test_list_objects_basic(s3):
    s3.put("/listbkt")
    s3.put("/listbkt/a.txt", body=b"1")
    s3.put("/listbkt/b.txt", body=b"2")
    r = s3.get("/listbkt")
    assert r.status_code == 200
    keys = [
        el.text
        for el in ET.fromstring(r.content).findall(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}Key"
        )
    ]
    assert "a.txt" in keys
    assert "b.txt" in keys


def test_list_objects_with_prefix(s3):
    s3.put("/prefbkt")
    s3.put("/prefbkt/img/a.png", body=b"")
    s3.put("/prefbkt/doc/b.pdf", body=b"")
    r = s3.get("/prefbkt", params={"prefix": "img/"})
    keys = [
        el.text
        for el in ET.fromstring(r.content).findall(
            ".//{http://s3.amazonaws.com/doc/2006-03-01/}Key"
        )
    ]
    assert all(k.startswith("img/") for k in keys)
    assert len(keys) == 1


def test_list_objects_delimiter(s3):
    s3.put("/delbkt")
    s3.put("/delbkt/a/1.txt", body=b"")
    s3.put("/delbkt/a/2.txt", body=b"")
    s3.put("/delbkt/b/3.txt", body=b"")
    s3.put("/delbkt/root.txt", body=b"")
    r = s3.get("/delbkt", params={"delimiter": "/"})
    root = ET.fromstring(r.content)
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    prefixes = [el.text for el in root.findall(f".//{{{ns}}}Prefix")]
    keys = [el.text for el in root.findall(f".//{{{ns}}}Key")]
    assert "a/" in prefixes
    assert "b/" in prefixes
    assert "root.txt" in keys


# ---------------------------------------------------------------------------
# 一括削除 (DELETE ?delete)
# ---------------------------------------------------------------------------

def test_batch_delete(s3):
    s3.put("/batch")
    s3.put("/batch/a.txt", body=b"1")
    s3.put("/batch/b.txt", body=b"2")
    s3.put("/batch/c.txt", body=b"3")

    body = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b'<Object><Key>a.txt</Key></Object>'
        b'<Object><Key>b.txt</Key></Object>'
        b'</Delete>'
    )
    r = s3.post("/batch", body=body, params={"delete": ""})
    assert r.status_code == 200

    assert s3.get("/batch/a.txt").status_code == 404
    assert s3.get("/batch/b.txt").status_code == 404
    assert s3.get("/batch/c.txt").status_code == 200   # 削除していない


# ---------------------------------------------------------------------------
# マルチパートアップロード
# ---------------------------------------------------------------------------

def test_multipart_full_lifecycle(s3):
    """開始 → パートアップロード → 完了 → 内容検証。"""
    s3.put("/mpbkt")

    # 1. 開始
    r = s3.post("/mpbkt/bigfile.bin", params={"uploads": ""})
    assert r.status_code == 200
    upload_id = ET.fromstring(r.content).findtext(
        "{http://s3.amazonaws.com/doc/2006-03-01/}UploadId"
    )
    assert upload_id

    # 2. パートアップロード
    part1 = b"A" * 100
    part2 = b"B" * 80
    r1 = s3.put(
        "/mpbkt/bigfile.bin",
        body=part1,
        params={"partNumber": "1", "uploadId": upload_id},
    )
    assert r1.status_code == 200
    etag1 = r1.headers["ETag"].strip('"')

    r2 = s3.put(
        "/mpbkt/bigfile.bin",
        body=part2,
        params={"partNumber": "2", "uploadId": upload_id},
    )
    assert r2.status_code == 200

    # 3. パート一覧
    rl = s3.get("/mpbkt/bigfile.bin", params={"uploadId": upload_id})
    assert rl.status_code == 200
    part_els = ET.fromstring(rl.content).findall(
        "{http://s3.amazonaws.com/doc/2006-03-01/}Part"
    )
    assert len(part_els) == 2

    # 4. 完了
    complete_xml = (
        b'<CompleteMultipartUpload>'
        b'<Part><PartNumber>1</PartNumber><ETag>' + etag1.encode() + b'</ETag></Part>'
        b'<Part><PartNumber>2</PartNumber><ETag></ETag></Part>'
        b'</CompleteMultipartUpload>'
    )
    rc = s3.post(
        "/mpbkt/bigfile.bin",
        body=complete_xml,
        params={"uploadId": upload_id},
    )
    assert rc.status_code == 200
    assert b"CompleteMultipartUploadResult" in rc.content

    # 5. 内容確認
    rg = s3.get("/mpbkt/bigfile.bin")
    assert rg.status_code == 200
    assert rg.content == part1 + part2


def test_multipart_abort(s3):
    s3.put("/mpabort")
    r = s3.post("/mpabort/f.bin", params={"uploads": ""})
    upload_id = ET.fromstring(r.content).findtext(
        "{http://s3.amazonaws.com/doc/2006-03-01/}UploadId"
    )
    ra = s3.delete("/mpabort/f.bin", params={"uploadId": upload_id})
    assert ra.status_code == 204


def test_multipart_upload_part_missing_returns_404(s3):
    r = s3.put("/ghost-bkt/f.bin", body=b"x",
               params={"partNumber": "1", "uploadId": "no-such-id"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# AWS Chunked 転送デコード
# ---------------------------------------------------------------------------

def test_aws_chunked_upload_decoded_correctly(s3):
    """
    AWS chunked フォーマット (STREAMING-*) をデコードして正しいデータを保存する。
    """
    s3.put("/chunked")

    payload = b"Hello, chunked world!"
    # AWS chunked 形式で手動エンコード
    chunk_size = len(payload)
    chunked_body = (
        f"{chunk_size:x};chunk-signature=dummy\r\n".encode()
        + payload
        + b"\r\n"
        + b"0;chunk-signature=dummy\r\n"
        + b"\r\n"
    )

    r = s3.put(
        "/chunked/f.txt",
        body=chunked_body,
        extra_headers={
            "Content-Encoding": "aws-chunked",
            "x-amz-decoded-content-length": str(len(payload)),
        },
    )
    assert r.status_code == 200

    rg = s3.get("/chunked/f.txt")
    assert rg.content == payload
