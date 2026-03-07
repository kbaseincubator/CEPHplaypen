"""Walk all visible S3 buckets/objects and write each object's size (bytes) to a file."""

import argparse
import getpass
import os
import sys

import socket
from urllib.parse import urlparse

import boto3
import socks
from botocore.exceptions import BotoCoreError, ClientError


def get_secret_key() -> str:
    """Return the S3 secret key from the environment or prompt securely."""
    secret = os.environ.get("S3_SECRET_KEY")
    if secret:
        return secret
    return getpass.getpass("S3 secret key: ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write S3 object sizes (one per line) to a file."
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("S3_ENDPOINT_URL"),
        help="S3 endpoint URL (or set S3_ENDPOINT_URL env var)",
    )
    parser.add_argument(
        "--access-key",
        default=os.environ.get("S3_ACCESS_KEY"),
        help="S3 access key ID (or set S3_ACCESS_KEY env var)",
    )
    parser.add_argument(
        "--output",
        default="object_sizes.txt",
        help="Output file path (default: object_sizes.txt)",
    )
    return parser.parse_args()


def iter_object_sizes(s3_client):
    """Yield (bucket, key, size) for every object across all visible buckets."""
    response = s3_client.list_buckets()
    buckets = [b["Name"] for b in response.get("Buckets", [])]

    for bucket in buckets:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                yield bucket, obj["Key"], obj["Size"]


def main() -> None:
    args = parse_args()

    if not args.access_key:
        print("Error: S3 access key required (--access-key or S3_ACCESS_KEY)", file=sys.stderr)
        sys.exit(1)

    secret_key = get_secret_key()

    # botocore does not support SOCKS proxies (https://github.com/boto/botocore/issues/2540).
    # Work around this by patching the socket directly via PySocks, and removing the
    # HTTPS_PROXY env var so botocore doesn't try (and fail) to handle it itself.
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy_url:
        parsed = urlparse(proxy_url)
        scheme_to_type = {
            "socks4": socks.SOCKS4,
            "socks4a": socks.SOCKS4,
            "socks5": socks.SOCKS5,
            "socks5h": socks.SOCKS5,
        }
        proxy_type = scheme_to_type.get(parsed.scheme)
        if proxy_type is not None:
            # botocore doesn't support SOCKS proxies; patch the socket directly and
            # remove the env var so botocore doesn't try (and fail) to handle it.
            socks.set_default_proxy(proxy_type, parsed.hostname, parsed.port)
            socket.socket = socks.socksocket
            os.environ.pop("HTTPS_PROXY", None)
            os.environ.pop("https_proxy", None)

    s3 = boto3.client(
        "s3",
        endpoint_url=args.endpoint_url,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        with open(args.output, "w") as out:
            count = 0
            for bucket, key, size in iter_object_sizes(s3):
                out.write(f"{size}\n")
                count += 1
        print(f"\nWrote {count} sizes to {args.output}")
    except (BotoCoreError, ClientError) as exc:
        print(f"S3 error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
