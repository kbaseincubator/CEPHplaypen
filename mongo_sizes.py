"""Walk all documents in a MongoDB collection and write the value of a key to a file."""

import argparse
import getpass
import os
import sys

from pymongo import MongoClient
from pymongo.errors import PyMongoError


def get_password() -> str:
    password = os.environ.get("MONGO_PASSWORD")
    if password:
        return password
    return getpass.getpass("MongoDB password: ")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a MongoDB field value from every document (one per line) to a file."
    )
    parser.add_argument(
        "--url", default=os.environ.get("MONGO_URL"),
        help="MongoDB host URL (or set MONGO_URL)",
    )
    parser.add_argument(
        "--username", default=os.environ.get("MONGO_USERNAME"),
        help="MongoDB username (or set MONGO_USERNAME)",
    )
    parser.add_argument(
        "--database", default=os.environ.get("MONGO_DATABASE"),
        help="Database name (or set MONGO_DATABASE)",
    )
    parser.add_argument(
        "--collection", default=os.environ.get("MONGO_COLLECTION"),
        help="Collection name (or set MONGO_COLLECTION)",
    )
    parser.add_argument(
        "--key", default=os.environ.get("MONGO_KEY"),
        help="Document field to extract (or set MONGO_KEY)",
    )
    parser.add_argument(
        "--unique-key",
        help="If set, only write the first document seen for each value of this field",
    )
    parser.add_argument(
        "--output", default="mongo_sizes.txt",
        help="Output file path (default: mongo_sizes.txt)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    missing = [name for name, val in [
        ("--url", args.url),
        ("--username", args.username),
        ("--database", args.database),
        ("--collection", args.collection),
        ("--key", args.key),
    ] if not val]
    if missing:
        print(f"Error: missing required arguments: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    password = get_password()

    try:
        client = MongoClient(
            args.url,
            username=args.username,
            password=password,
            authSource=args.database,
            directConnection=True,
        )
        collection = client[args.database][args.collection]

        projection = {args.key: 1}
        if args.unique_key:
            projection[args.unique_key] = 1

        seen = set()
        with open(args.output, "w") as out:
            count = 0
            ttlcount = 0
            for doc in collection.find({}, projection):
                ttlcount += 1
                if args.key not in doc:
                    print(f"Error: document missing field '{args.key}': {doc}", file=sys.stderr)
                    sys.exit(1)
                if args.unique_key:
                    if args.unique_key not in doc:
                        print(
                            f"Error: document missing field '{args.unique_key}': {doc}",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    unique_val = doc[args.unique_key]
                    if unique_val in seen:
                        continue
                    seen.add(unique_val)
                out.write(f"{doc[args.key]}\n")
                count += 1

        print(f"Wrote {count:,}/{ttlcount:,} values to {args.output}")
    except PyMongoError as exc:
        print(f"MongoDB error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
