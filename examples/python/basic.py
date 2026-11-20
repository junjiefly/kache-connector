from __future__ import annotations

import argparse
import json
from pathlib import Path

from kvcache_connector import KVCacheConnector, KVChunk


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal KV Cache connector example")
    parser.add_argument("--config-file", required=True, help="Connector config JSON")
    parser.add_argument("--key", default="example/basic")
    parser.add_argument("--data", default="hello kv cache")
    args = parser.parse_args()

    config = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
    payload = args.data.encode("utf-8")

    with KVCacheConnector.from_config(config) as connector:
        put_results = connector.put_batch([KVChunk(key=args.key, data=payload)])
        if not put_results[0].ok:
            raise RuntimeError(f"put failed: {put_results[0].error_code}")
        print(f"PUT {args.key}: {len(payload)} bytes")

        print("GET:", connector.get_values([args.key, "example/missing"]))

        delete_results = connector.delete_batch([args.key])
        if not delete_results[0].deleted:
            raise RuntimeError(f"delete failed: {delete_results[0].error_code}")
        print(f"DELETE {args.key}: OK")


if __name__ == "__main__":
    main()
