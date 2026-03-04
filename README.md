# nekono3s

Lightweight S3-compatible object storage server built with FastAPI.
Uses the local filesystem as the storage backend and xattr for metadata (jclouds filesystem-nio2 compatible).

## Features

- **S3-compatible API** — works with AWS SDK, boto3, s3cmd, rclone, etc.
- **AWS Signature V4 / V2** authentication
- **Multipart upload** support
- **Range requests** (partial GET)
- **Batch delete**
- **Docker-ready** — single container, no external dependencies

## Supported Operations

| Category | Operations |
|---|---|
| Service | `GET /` — List buckets |
| Bucket | PUT, HEAD, DELETE, GET (list objects), GET ?acl, GET ?location |
| Object | PUT, GET (with Range), HEAD, DELETE, POST ?delete (batch) |
| Multipart | Initiate, Upload Part, Complete, Abort, List Parts |

## Quick Start

### Docker Compose

```bash
docker compose up -d
```

The server starts on port **8080** with default credentials:

- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
- **Region**: `us-east-1`

### Docker (standalone)

```bash
docker run -d -p 8080:8080 \
  -e S3_ACCESS_KEY_ID=mykey \
  -e S3_SECRET_ACCESS_KEY=mysecret \
  -v s3data:/data \
  ghcr.io/nananek/nekono3s:latest
```

## Configuration

All settings are configured via environment variables with the `S3_` prefix:

| Variable | Default | Description |
|---|---|---|
| `S3_ACCESS_KEY_ID` | `minioadmin` | Access key for authentication |
| `S3_SECRET_ACCESS_KEY` | `minioadmin` | Secret key for authentication |
| `S3_STORAGE_PATH` | `/data` | Path to store objects |
| `S3_REGION` | `us-east-1` | Region name |
| `S3_XATTR_JCLOUDS_COMPAT` | `false` | Use s3proxy/jclouds xattr format (`user.user.*`) |

### Migrating from s3proxy

If you are migrating from s3proxy (jclouds filesystem-nio2), enable jclouds-compatible xattr format so nekono3s can read existing metadata:

```bash
S3_XATTR_JCLOUDS_COMPAT=true
```

s3proxy stores xattr keys as `user.user.content-type`, `user.user.content-md5`, etc. With this option enabled, nekono3s uses the same format. Back up xattr when copying files (`tar --xattrs --xattrs-include='*'` or `rsync -X`).

## Usage with AWS CLI

```bash
aws --endpoint-url http://localhost:8080 s3 mb s3://mybucket
aws --endpoint-url http://localhost:8080 s3 cp file.txt s3://mybucket/
aws --endpoint-url http://localhost:8080 s3 ls s3://mybucket/
```

## Usage with boto3

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:8080",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
)

s3.create_bucket(Bucket="mybucket")
s3.put_object(Bucket="mybucket", Key="hello.txt", Body=b"Hello!")
```

## Development

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## License

MIT
