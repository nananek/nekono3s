"""
FilesystemStorage の単体テスト。

xattr が使えない環境では xattr 依存テストをスキップする。
"""

import os
import tempfile

import pytest

from app.storage import (
    XATTR_CONTENT_MD5,
    XATTR_CONTENT_TYPE,
    FilesystemStorage,
    ObjectMetadata,
)


# ---------------------------------------------------------------------------
# xattr が利用可能かチェック
# ---------------------------------------------------------------------------

def _xattr_available() -> bool:
    try:
        with tempfile.NamedTemporaryFile(dir="/tmp", delete=False) as f:
            name = f.name
        os.setxattr(name, "user.test", b"1")
        os.removexattr(name, "user.test")
        os.unlink(name)
        return True
    except (OSError, AttributeError):
        return False


xattr_only = pytest.mark.skipif(
    not _xattr_available(), reason="xattr not supported on this filesystem"
)


# ---------------------------------------------------------------------------
# バケット操作
# ---------------------------------------------------------------------------

def test_bucket_create_and_exists(storage):
    assert not storage.bucket_exists("testbucket")
    storage.create_bucket("testbucket")
    assert storage.bucket_exists("testbucket")


def test_bucket_list_empty(storage):
    assert storage.list_buckets() == []


def test_bucket_list_after_create(storage):
    storage.create_bucket("b1")
    storage.create_bucket("b2")
    names = [b["name"] for b in storage.list_buckets()]
    assert names == ["b1", "b2"]


def test_delete_empty_bucket(storage):
    storage.create_bucket("delbucket")
    storage.delete_bucket("delbucket")
    assert not storage.bucket_exists("delbucket")


def test_delete_nonempty_bucket_raises(storage):
    storage.create_bucket("full")
    storage.put_object("full", "key.txt", b"data", ObjectMetadata())
    with pytest.raises(ValueError, match="BucketNotEmpty"):
        storage.delete_bucket("full")


def test_delete_missing_bucket_raises(storage):
    with pytest.raises(FileNotFoundError):
        storage.delete_bucket("missing")


# ---------------------------------------------------------------------------
# オブジェクト PUT / HEAD
# ---------------------------------------------------------------------------

def test_put_and_head_roundtrip(storage):
    storage.create_bucket("b")
    etag = storage.put_object("b", "hello.txt", b"world", ObjectMetadata(
        content_type="text/plain"
    ))
    size, meta = storage.head_object("b", "hello.txt")
    assert size == 5
    assert meta.content_type == "text/plain"
    assert meta.etag == etag


def test_put_returns_hex_md5(storage):
    import hashlib
    storage.create_bucket("b")
    data = b"test data"
    etag = storage.put_object("b", "k", data, ObjectMetadata())
    assert etag == hashlib.md5(data).hexdigest()


def test_head_missing_object_raises(storage):
    storage.create_bucket("b")
    with pytest.raises(FileNotFoundError):
        storage.head_object("b", "missing.txt")


def test_get_object_path_returns_path(storage):
    storage.create_bucket("b")
    storage.put_object("b", "f.txt", b"x", ObjectMetadata())
    path = storage.get_object_path("b", "f.txt")
    assert path.is_file()


# ---------------------------------------------------------------------------
# xattr: jclouds filesystem-nio2 互換フォーマット検証
# ---------------------------------------------------------------------------

@xattr_only
def test_xattr_content_md5_is_raw_bytes(storage):
    """content-md5 xattr は生の 16 バイト (hex 文字列ではない)。"""
    import hashlib
    storage.create_bucket("b")
    data = b"binary data"
    storage.put_object("b", "k", data, ObjectMetadata())
    obj_path = storage.get_object_path("b", "k")

    raw = os.getxattr(str(obj_path), XATTR_CONTENT_MD5)
    assert len(raw) == 16, "MD5 は 16 バイトのバイナリ"
    assert raw == hashlib.md5(data).digest()


@xattr_only
def test_xattr_content_type_is_utf8_string(storage):
    """content-type xattr は UTF-8 文字列。"""
    storage.create_bucket("b")
    storage.put_object("b", "k", b"x", ObjectMetadata(content_type="image/png"))
    obj_path = storage.get_object_path("b", "k")

    raw = os.getxattr(str(obj_path), XATTR_CONTENT_TYPE)
    assert raw == b"image/png"


@xattr_only
def test_user_metadata_stored_without_prefix(storage):
    """x-amz-meta-foo は xattr に 'user.foo' として格納される (プレフィックス除去済み)。"""
    storage.create_bucket("b")
    storage.put_object("b", "k", b"x", ObjectMetadata(
        user_metadata={"foo": "bar", "author": "alice"}
    ))
    obj_path = storage.get_object_path("b", "k")

    attrs = os.listxattr(str(obj_path))
    assert "user.foo" in attrs
    assert "user.author" in attrs
    # x-amz-meta- プレフィックス付きの xattr は存在しない
    assert not any(a.startswith("user.x-amz-meta-") for a in attrs)


@xattr_only
def test_user_metadata_roundtrip(storage):
    """ユーザーメタデータが読み書きで一致する。"""
    storage.create_bucket("b")
    storage.put_object("b", "k", b"x", ObjectMetadata(
        user_metadata={"env": "prod", "version": "2"}
    ))
    _, meta = storage.head_object("b", "k")
    assert meta.user_metadata == {"env": "prod", "version": "2"}


@xattr_only
def test_all_system_metadata_roundtrip(storage):
    """システムメタデータが xattr 経由で正確に往復する。"""
    storage.create_bucket("b")
    orig = ObjectMetadata(
        content_type="application/pdf",
        content_disposition="attachment; filename=doc.pdf",
        content_encoding="gzip",
        content_language="ja",
        expires="Thu, 01 Jan 2099 00:00:00 GMT",
        cache_control="max-age=3600",
    )
    storage.put_object("b", "k", b"pdf", orig)
    _, meta = storage.head_object("b", "k")
    assert meta.content_type == "application/pdf"
    assert meta.content_disposition == "attachment; filename=doc.pdf"
    assert meta.content_encoding == "gzip"
    assert meta.content_language == "ja"
    assert meta.expires == "Thu, 01 Jan 2099 00:00:00 GMT"
    assert meta.cache_control == "max-age=3600"


@xattr_only
def test_read_externally_written_xattr(storage, tmp_path):
    """s3proxy が書いた xattr を正しく読める (外部ツールでセットしたケース)。"""
    import hashlib
    storage.create_bucket("ext")
    obj_path = storage.get_object_path.__func__  # just need the path
    # 直接ファイル作成
    p = storage._object_path("ext", "obj.bin")
    p.parent.mkdir(parents=True, exist_ok=True)
    data = b"external"
    p.write_bytes(data)
    # s3proxy が書く形式で xattr を手動セット
    os.setxattr(str(p), XATTR_CONTENT_TYPE, b"application/octet-stream")
    os.setxattr(str(p), XATTR_CONTENT_MD5, hashlib.md5(data).digest())
    os.setxattr(str(p), "user.mykey", "myvalue".encode("utf-8"))

    _, meta = storage.head_object("ext", "obj.bin")
    assert meta.content_type == "application/octet-stream"
    assert meta.content_md5 == hashlib.md5(data).digest()
    assert meta.user_metadata == {"mykey": "myvalue"}


# ---------------------------------------------------------------------------
# オブジェクト一覧
# ---------------------------------------------------------------------------

def test_list_objects_basic(storage):
    storage.create_bucket("b")
    storage.put_object("b", "a.txt", b"1", ObjectMetadata())
    storage.put_object("b", "b.txt", b"2", ObjectMetadata())
    result = storage.list_objects("b")
    keys = [o["key"] for o in result["objects"]]
    assert "a.txt" in keys
    assert "b.txt" in keys


def test_list_objects_prefix(storage):
    storage.create_bucket("b")
    storage.put_object("b", "img/a.png", b"", ObjectMetadata())
    storage.put_object("b", "img/b.png", b"", ObjectMetadata())
    storage.put_object("b", "doc/a.pdf", b"", ObjectMetadata())
    result = storage.list_objects("b", prefix="img/")
    keys = [o["key"] for o in result["objects"]]
    assert all(k.startswith("img/") for k in keys)
    assert len(keys) == 2


def test_list_objects_delimiter_common_prefixes(storage):
    storage.create_bucket("b")
    storage.put_object("b", "a/x.txt", b"", ObjectMetadata())
    storage.put_object("b", "a/y.txt", b"", ObjectMetadata())
    storage.put_object("b", "b/z.txt", b"", ObjectMetadata())
    storage.put_object("b", "root.txt", b"", ObjectMetadata())
    result = storage.list_objects("b", delimiter="/")
    keys = [o["key"] for o in result["objects"]]
    assert "root.txt" in keys
    assert "a/x.txt" not in keys
    assert "a/" in result["common_prefixes"]
    assert "b/" in result["common_prefixes"]


def test_list_objects_max_keys_truncated(storage):
    storage.create_bucket("b")
    for i in range(5):
        storage.put_object("b", f"f{i}.txt", b"", ObjectMetadata())
    result = storage.list_objects("b", max_keys=3)
    assert len(result["objects"]) == 3
    assert result["is_truncated"] is True
    assert result["next_marker"] != ""


def test_list_objects_marker(storage):
    storage.create_bucket("b")
    for i in range(4):
        storage.put_object("b", f"f{i}.txt", b"", ObjectMetadata())
    first = storage.list_objects("b", max_keys=2)
    marker = first["next_marker"]
    second = storage.list_objects("b", max_keys=2, marker=marker)
    keys_all = (
        [o["key"] for o in first["objects"]]
        + [o["key"] for o in second["objects"]]
    )
    assert len(set(keys_all)) == 4


# ---------------------------------------------------------------------------
# 削除
# ---------------------------------------------------------------------------

def test_delete_object(storage):
    storage.create_bucket("b")
    storage.put_object("b", "del.txt", b"x", ObjectMetadata())
    assert storage.delete_object("b", "del.txt") is True
    with pytest.raises(FileNotFoundError):
        storage.head_object("b", "del.txt")


def test_delete_nonexistent_returns_false(storage):
    storage.create_bucket("b")
    assert storage.delete_object("b", "ghost.txt") is False


def test_delete_cleans_empty_parent_dirs(storage):
    storage.create_bucket("b")
    storage.put_object("b", "deep/dir/file.txt", b"x", ObjectMetadata())
    storage.delete_object("b", "deep/dir/file.txt")
    # 空になった親ディレクトリは削除される
    assert not (storage._bucket_path("b") / "deep").exists()
