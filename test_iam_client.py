"""
Integration test script for S3IAMClient against a live S3-compatible IAM endpoint.

Creates isolated test resources using a random suffix, exercises all client methods,
checks final state, then cleans up. Exits non-zero on any failure.
"""

import argparse
import asyncio
import getpass
import os
import sys
import uuid

from botocore.exceptions import ClientError

from s3_iam_client import S3IAMClient

_POLICY_NAME = "test-policy"

_USER_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "*"}],
}
_GROUP_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "*"}],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Integration tests for S3IAMClient.")
    parser.add_argument(
        "--endpoint-url", default=os.environ.get("S3_ENDPOINT_URL"),
        help="IAM endpoint URL (or set S3_ENDPOINT_URL)",
    )
    parser.add_argument(
        "--access-key", default=os.environ.get("S3_ACCESS_KEY"),
        help="Access key ID (or set S3_ACCESS_KEY)",
    )
    return parser.parse_args()


def get_secret_key() -> str:
    secret = os.environ.get("S3_SECRET_KEY")
    if secret:
        return secret
    return getpass.getpass("S3 secret key: ")


def passed(msg: str) -> None:
    print(f"  PASS: {msg}")


def failed(msg: str) -> None:
    print(f"  FAIL: {msg}")
    sys.exit(1)


def check(condition: bool, msg: str) -> None:
    if condition:
        passed(msg)
    else:
        failed(msg)


async def find_key_limit(client: S3IAMClient, username: str) -> int:
    """Create keys until the server rejects, returning the limit."""
    count = 0
    while True:
        try:
            await client.create_access_key(username)
            count += 1
        except ClientError:
            return count



async def cleanup(
    client: S3IAMClient, username: str, group_name: str
) -> None:
    print("\n--- Cleaning up ---")
    for label, coro in [
        (f"user {username}", client.delete_user(username)),
        (f"group {group_name}", client.delete_group(group_name)),
    ]:
        try:
            await coro
            print(f"  Deleted {label}")
        except ClientError as e:
            print(f"  Warning: could not delete {label}: {e}")


async def run_tests(client: S3IAMClient, username: str, group_name: str) -> None:
    print(f"\n--- Creating user and group ---")
    await client.create_user(username)
    passed(f"create_user({username!r})")
    await client.create_group(group_name)
    passed(f"create_group({group_name!r})")

    print(f"\n--- Access key limit ---")
    limit = await find_key_limit(client, username)
    print(f"  Key limit reported by server: {limit}")
    check(limit >= 2, "server allows at least 2 access keys")
    # Clean up test keys by rotating down to a single key
    await client.rotate_access_key(username)

    print(f"\n--- Key rotation ---")
    key_id, secret = await client.rotate_access_key(username)
    check(bool(key_id) and bool(secret), "rotate_access_key returns a key pair")

    print(f"\n--- User inline policy ---")
    await client.set_user_policy(username, _POLICY_NAME, _USER_POLICY)
    passed("set_user_policy")
    result = await client.get_user_policy(username, _POLICY_NAME)
    check(result == _USER_POLICY, "get_user_policy round-trips correctly")

    print(f"\n--- Group inline policy ---")
    await client.set_group_policy(group_name, _POLICY_NAME, _GROUP_POLICY)
    passed("set_group_policy")
    result = await client.get_group_policy(group_name, _POLICY_NAME)
    check(result == _GROUP_POLICY, "get_group_policy round-trips correctly")

    print(f"\n--- Group membership ---")
    await client.add_user_to_group(username, group_name)
    passed("add_user_to_group")
    users = await client.list_users_in_group(group_name)
    check(username in users, "user present in group after add")

    await client.remove_user_from_group(username, group_name)
    passed("remove_user_from_group")
    users = await client.list_users_in_group(group_name)
    check(username not in users, "user absent from group after remove")


async def main() -> None:
    args = parse_args()

    missing = [n for n, v in [("--endpoint-url", args.endpoint_url),
                               ("--access-key", args.access_key)] if not v]
    if missing:
        print(f"Error: missing required arguments: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    secret_key = get_secret_key()

    suffix = uuid.uuid4().hex[:8]
    username = f"cephtest-{suffix}-user"
    group_name = f"cephtest-{suffix}-group"
    path_prefix = f"/cephtest-{suffix}/"

    print(f"Endpoint: {args.endpoint_url}")
    print(f"Access key: {args.access_key}")
    print(f"Test resources: user={username!r}, group={group_name!r}, path={path_prefix!r}")

    async with S3IAMClient(
        args.endpoint_url, args.access_key, secret_key, path_prefix
    ) as client:
        try:
            await run_tests(client, username, group_name)
        finally:
            await cleanup(client, username, group_name)

    print("\nAll tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
