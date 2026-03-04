"""S3 XML response builders."""

from xml.etree.ElementTree import Element, SubElement, tostring

_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
_DECL = b'<?xml version="1.0" encoding="UTF-8"?>'


def _root(tag: str) -> Element:
    el = Element(tag)
    el.set("xmlns", _NS)
    return el


def _xml(el: Element) -> bytes:
    return _DECL + tostring(el, encoding="unicode").encode()


# ---------------------------------------------------------------------------

def list_all_my_buckets_result(owner_id: str, owner_name: str, buckets: list[dict]) -> bytes:
    root = _root("ListAllMyBucketsResult")
    owner = SubElement(root, "Owner")
    SubElement(owner, "ID").text = owner_id
    SubElement(owner, "DisplayName").text = owner_name
    bs = SubElement(root, "Buckets")
    for b in buckets:
        be = SubElement(bs, "Bucket")
        SubElement(be, "Name").text = b["name"]
        SubElement(be, "CreationDate").text = b["creation_date"]
    return _xml(root)


def list_bucket_result(
    bucket: str,
    prefix: str,
    delimiter: str,
    max_keys: int,
    objects: list,
    common_prefixes: list,
    is_truncated: bool,
    marker: str = "",
    next_marker: str = "",
) -> bytes:
    root = _root("ListBucketResult")
    SubElement(root, "Name").text = bucket
    SubElement(root, "Prefix").text = prefix
    SubElement(root, "Marker").text = marker
    SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        SubElement(root, "Delimiter").text = delimiter
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"
    if is_truncated and next_marker:
        SubElement(root, "NextMarker").text = next_marker

    for obj in objects:
        c = SubElement(root, "Contents")
        SubElement(c, "Key").text = obj["key"]
        SubElement(c, "LastModified").text = obj["last_modified"]
        SubElement(c, "ETag").text = obj["etag"]
        SubElement(c, "Size").text = str(obj["size"])
        SubElement(c, "StorageClass").text = obj.get("storage_class", "STANDARD")
        owner = SubElement(c, "Owner")
        SubElement(owner, "ID").text = "owner"
        SubElement(owner, "DisplayName").text = "owner"

    for cp in common_prefixes:
        cpe = SubElement(root, "CommonPrefixes")
        SubElement(cpe, "Prefix").text = cp

    return _xml(root)


def error_response(code: str, message: str, resource: str = "", request_id: str = "000") -> bytes:
    root = Element("Error")
    SubElement(root, "Code").text = code
    SubElement(root, "Message").text = message
    SubElement(root, "Resource").text = resource
    SubElement(root, "RequestId").text = request_id
    return _DECL + tostring(root, encoding="unicode").encode()


def delete_result(deleted: list[str], errors: list[dict] | None = None) -> bytes:
    root = _root("DeleteResult")
    for key in deleted:
        d = SubElement(root, "Deleted")
        SubElement(d, "Key").text = key
    for err in (errors or []):
        e = SubElement(root, "Error")
        SubElement(e, "Key").text = err.get("key", "")
        SubElement(e, "Code").text = err.get("code", "InternalError")
        SubElement(e, "Message").text = err.get("message", "")
    return _xml(root)


def initiate_multipart_upload_result(bucket: str, key: str, upload_id: str) -> bytes:
    root = _root("InitiateMultipartUploadResult")
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id
    return _xml(root)


def complete_multipart_upload_result(location: str, bucket: str, key: str, etag: str) -> bytes:
    root = _root("CompleteMultipartUploadResult")
    SubElement(root, "Location").text = location
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "Key").text = key
    SubElement(root, "ETag").text = f'"{etag}"'
    return _xml(root)


def list_parts_result(bucket: str, key: str, upload_id: str, parts: list) -> bytes:
    root = _root("ListPartsResult")
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id
    SubElement(root, "IsTruncated").text = "false"
    for part in parts:
        p = SubElement(root, "Part")
        SubElement(p, "PartNumber").text = str(part["part_number"])
        SubElement(p, "LastModified").text = part["last_modified"]
        SubElement(p, "ETag").text = part["etag"]
        SubElement(p, "Size").text = str(part["size"])
    return _xml(root)


def list_multipart_uploads_result(bucket: str) -> bytes:
    root = _root("ListMultipartUploadsResult")
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "IsTruncated").text = "false"
    return _xml(root)


def create_bucket_configuration(location: str) -> bytes:
    root = _root("CreateBucketConfiguration")
    SubElement(root, "LocationConstraint").text = location
    return _xml(root)
