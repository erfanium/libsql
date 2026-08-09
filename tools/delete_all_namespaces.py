#!/usr/bin/env python3
"""Delete every namespace/database on the miniturso admin API.

Lists all databases via GET /api/databases and deletes them one by one
with DELETE /api/databases/:id. Prints progress and verifies the list is
empty at the end. Stdlib only.

Usage:
    python3 tools/delete_all_namespaces.py
    python3 tools/delete_all_namespaces.py --url http://127.0.0.1:3001 --admin-key <key>
    python3 tools/delete_all_namespaces.py --dry-run
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default="http://127.0.0.1:3001", help="admin API base URL")
    ap.add_argument("--admin-key", default="miniturso-admin-key-change-me")
    ap.add_argument("--dry-run", action="store_true", help="list namespaces but delete nothing")
    ap.add_argument("--timeout", type=float, default=60.0, help="per-request timeout (s)")
    args = ap.parse_args()

    auth = {"Authorization": "Bearer " + args.admin_key}
    base = args.url

    def request(method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            base + path,
            data=data,
            headers={"Content-Type": "application/json", **auth},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=args.timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    status, raw = request("GET", "/api/databases")
    if status != 200:
        print(f"error: GET /api/databases -> HTTP {status}: {raw[:200]}")
        sys.exit(1)
    namespaces = [d["namespace"] for d in json.loads(raw)["databases"]]
    print(f"found {len(namespaces)} namespace(s) on {base}")

    if args.dry_run:
        for ns in namespaces:
            print(f"  would delete {ns}")
        print("dry run, nothing deleted")
        return

    ok = fail = 0
    t0 = time.time()
    for i, ns in enumerate(namespaces, 1):
        code, raw = request("DELETE", f"/api/databases/{ns}")
        if code == 200:
            ok += 1
        else:
            fail += 1
            print(f"  FAILED {ns}: HTTP {code}: {raw[:150]}")
        if i % 50 == 0 or i == len(namespaces):
            print(f"  {i}/{len(namespaces)} processed ({ok} ok, {fail} failed), {time.time() - t0:.0f}s")

    status, raw = request("GET", "/api/databases")
    remaining = len(json.loads(raw)["databases"]) if status == 200 else -1
    print(f"done in {time.time() - t0:.0f}s: {ok} deleted, {fail} failed, {remaining} remaining")
    sys.exit(0 if fail == 0 and remaining == 0 else 1)


if __name__ == "__main__":
    main()
