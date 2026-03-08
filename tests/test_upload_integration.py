"""
実サーバー経由のアップロード統合テスト。

TestClient ではなく uvicorn を実際に起動し、httpx で HTTP リクエストを送る。
BaseHTTPMiddleware によるストリーム消費問題の再現・検証を目的とする。
"""

import hashlib
import multiprocessing
import time

import httpx
import pytest
import uvicorn

# conftest.py が先に S3_ 環境変数をセットしてくれる
from tests.conftest import V4Signer


def _run_server(storage_path: str, port: int):
    """子プロセスで uvicorn を起動する。"""
    import os
    os.environ["S3_STORAGE_PATH"] = storage_path
    os.environ["S3_MULTIPART_PATH"] = storage_path + "/.multipart"

    # Settings はモジュールロード時に評価済みなので再ロード
    import importlib
    import app.config
    importlib.reload(app.config)
    import app.main
    importlib.reload(app.main)

    uvicorn.run(app.main.app, host="127.0.0.1", port=port, log_level="error")


class _LiveS3Client:
    """実サーバーに対して署名付きリクエストを発行するクライアント。"""

    def __init__(self, base_url: str, signer: V4Signer):
        self._base = base_url
        self._signer = signer
        self._http = httpx.Client(base_url=base_url, timeout=30)

    def request(self, method, path, body=b"", params=None, extra_headers=None):
        qs = ""
        if params:
            qs = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        url = self._base + path + qs
        headers = self._signer.sign(method, url, body, extra_headers)
        return self._http.request(method, path, content=body,
                                  headers=headers, params=params)

    def put(self, path, body=b"", **kw):
        return self.request("PUT", path, body, **kw)

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def close(self):
        self._http.close()


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    """uvicorn を子プロセスで起動し、テスト後に停止する。"""
    data_dir = str(tmp_path_factory.mktemp("live_data"))
    port = 19876

    proc = multiprocessing.Process(target=_run_server, args=(data_dir, port), daemon=True)
    proc.start()

    # サーバー起動待ち
    base_url = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            httpx.get(base_url + "/", timeout=0.5)
            break
        except httpx.ConnectError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("Server did not start")

    yield base_url

    proc.kill()
    proc.join(timeout=3)


@pytest.fixture(scope="module")
def live_s3(live_server):
    client = _LiveS3Client(live_server, V4Signer())
    yield client
    client.close()


# ---------------------------------------------------------------------------
# テスト
# ---------------------------------------------------------------------------

def test_put_get_small_file(live_s3):
    """小さなファイルの PUT → GET ラウンドトリップ。"""
    live_s3.put("/integ")
    data = b"hello world"
    r = live_s3.put("/integ/small.txt", body=data,
                    extra_headers={"Content-Type": "text/plain"})
    assert r.status_code == 200

    r = live_s3.get("/integ/small.txt")
    assert r.status_code == 200
    assert r.content == data


def test_put_get_large_binary(live_s3):
    """1MB バイナリの PUT → GET。ボディが途中で切れないことを確認。"""
    live_s3.put("/integ2")
    data = bytes(range(256)) * 4096  # 1MB
    expected_md5 = hashlib.md5(data).hexdigest()

    r = live_s3.put("/integ2/large.bin", body=data)
    assert r.status_code == 200
    etag = r.headers.get("ETag", "").strip('"')
    assert etag == expected_md5

    r = live_s3.get("/integ2/large.bin")
    assert r.status_code == 200
    assert len(r.content) == len(data)
    assert hashlib.md5(r.content).hexdigest() == expected_md5


def test_put_get_video_size(live_s3):
    """10MB の擬似動画ファイル。サイズと内容が一致することを確認。"""
    live_s3.put("/integ3")
    data = b"\x00\x00\x00\x1cftypisom" + b"\xab" * (10 * 1024 * 1024 - 12)
    expected_md5 = hashlib.md5(data).hexdigest()

    r = live_s3.put("/integ3/video.mp4", body=data,
                    extra_headers={"Content-Type": "video/mp4"})
    assert r.status_code == 200

    r = live_s3.get("/integ3/video.mp4")
    assert r.status_code == 200
    assert len(r.content) == len(data), f"Expected {len(data)} bytes, got {len(r.content)}"
    assert hashlib.md5(r.content).hexdigest() == expected_md5
