FROM python:3.11-slim

WORKDIR /app

# attr パッケージ: getfattr/setfattr コマンド（デバッグ用）
RUN apt-get update && apt-get install -y --no-install-recommends attr \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

# s3proxy 互換の UID/GID 101 で実行
RUN groupadd -g 101 s3 && useradd -u 101 -g 101 -m s3 \
    && mkdir -p /data && chown 101:101 /data

USER 101

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
