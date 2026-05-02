#!/usr/bin/env python3
"""
Dump IVFFlat index metadata for public.part / public.partsupp on database tpch10:

  - nlist (lists count) and vector dimensions from the metapage
  - For each list: cluster id (0..nlist-1), center vector, number of indexed tuples in that list

Requires the pageinspect extension (get_raw_page), typically superuser:

  CREATE EXTENSION IF NOT EXISTS pageinspect;

Only IVFFlat indexes whose indexed column type is pgvector ``vector`` are supported
(center layout matches src/ivfflat.h IvfflatListData + Vector).

Connection: same as other dev scripts — PSQL or /data/dzh/postgresql/bin/psql,
PGDATABASE, PGHOST, PGPORT, PGUSER.

Performance: counting ``vector_count`` walks every index data page. Each page is one
``get_raw_page`` call; the default backend is one ``psql`` subprocess per page (very slow).
Install ``psycopg2-binary`` (optional) to reuse one DB connection, or pass
``--no-count-vectors`` to only dump nlist + centers (much faster). Progress prints to stderr.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import struct
import subprocess
import sys
import time
from typing import Any, Callable

# From ivfflat.h
IVFFLAT_MAGIC = 0x14FF1A7
IVFFLAT_PAGE_ID = 0xFF84
INVALID_BLOCK = 0xFFFFFFFF

LP_NORMAL = 1


def _resolve_psql() -> str:
    p = os.environ.get("PSQL", "").strip()
    if p and os.path.isfile(p) and os.access(p, os.X_OK):
        return p
    for c in ("/data/dzh/postgresql/bin/psql",):
        if os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    w = subprocess.run(["which", "psql"], capture_output=True, text=True)
    if w.returncode == 0 and w.stdout.strip():
        return w.stdout.strip()
    raise RuntimeError("Set PSQL to psql path or install client tools.")


def _pg_env(database: str) -> dict[str, str]:
    e = os.environ.copy()
    e.setdefault("PGHOST", "127.0.0.1")
    e.setdefault("PGPORT", "5432")
    e.setdefault("PGUSER", "dzh")
    e["PGDATABASE"] = database
    return e


def _psql_sql(sql: str, database: str) -> str:
    env = _pg_env(database)
    r = subprocess.run(
        [
            _resolve_psql(),
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            env["PGDATABASE"],
            "-h",
            env["PGHOST"],
            "-p",
            env["PGPORT"],
            "-U",
            env["PGUSER"],
            "-t",
            "-A",
        ],
        input=sql.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        check=False,
    )
    if r.returncode != 0:
        sys.stderr.buffer.write(r.stderr)
        raise RuntimeError(f"psql failed ({r.returncode})")
    return r.stdout.decode()


def _hex_row_to_str(hexed: Any) -> str:
    if isinstance(hexed, memoryview):
        return bytes(hexed).decode()
    if isinstance(hexed, (bytes, bytearray)):
        return hexed.decode()
    return str(hexed).strip()


def _make_page_fetcher(
    database: str, idx_reg: str
) -> tuple[Callable[[int], bytes], Callable[[], None], str]:
    """
    Returns (fetch_blk, close, backend_name). backend_name is 'psycopg2' or 'psql'.
    """
    safe = idx_reg.replace("'", "''")

    try:
        import psycopg2  # type: ignore[import-not-found]

        conn = psycopg2.connect(
            host=os.environ.get("PGHOST", "127.0.0.1"),
            port=int(os.environ.get("PGPORT", "5432")),
            user=os.environ.get("PGUSER", "dzh"),
            dbname=database,
        )
        conn.autocommit = True
        cur = conn.cursor()

        def fetch_blk(blk: int) -> bytes:
            cur.execute(
                "SELECT encode(get_raw_page(%s::text, 'main'::text, %s::int), 'hex')",
                (safe, blk),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"get_raw_page returned no row for block {blk}")
            hexed = _hex_row_to_str(row[0])
            if not re.fullmatch(r"[0-9a-fA-F]+", hexed) or len(hexed) % 2 != 0:
                raise RuntimeError(f"bad hex for block {blk}: {hexed[:80]!r}")
            return bytes.fromhex(hexed)

        def close() -> None:
            cur.close()
            conn.close()

        return fetch_blk, close, "psycopg2"
    except Exception:
        pass

    def fetch_blk(blk: int) -> bytes:
        hexed = _psql_sql(
            f"SELECT encode(get_raw_page('{safe}'::text, 'main'::text, {blk}::int), 'hex');",
            database,
        ).strip()
        if not re.fullmatch(r"[0-9a-fA-F]+", hexed) or len(hexed) % 2 != 0:
            raise RuntimeError(f"bad hex for block {blk}: {hexed[:80]!r}")
        return bytes.fromhex(hexed)

    return fetch_blk, (lambda: None), "psql"


def _page_header(page: bytes) -> tuple[int, int, int]:
    """Return (pd_lower, pd_upper, pd_special) from PageHeaderData."""
    if len(page) < 24:
        raise ValueError("page too small")
    lower, upper, special = struct.unpack_from("<HHH", page, 12)
    return lower, upper, special


def _item_id_unpack(lp: int) -> tuple[int, int, int]:
    off = lp & 0x7FFF
    flags = (lp >> 15) & 0x3
    length = (lp >> 17) & 0x7FFF
    return off, flags, length


def _read_item_ids(page: bytes, lower: int) -> list[tuple[int, int, int]]:
    """Return list of (offset, flags, length) for item slots 1..N (1-based PG convention)."""
    hdr_end = 24
    if lower <= hdr_end or (lower - hdr_end) % 4 != 0:
        raise ValueError(f"bad pd_lower={lower}")
    nslots = (lower - hdr_end) // 4
    out: list[tuple[int, int, int]] = []
    for i in range(nslots):
        lp = struct.unpack_from("<I", page, hdr_end + i * 4)[0]
        out.append(_item_id_unpack(lp))
    # PG: offset 1 = first slot -> skip index 0 if unused? Usually slot 0 unused on index pages.
    # We return all slots; caller uses 1-based offno = idx+1 and skips LP_UNUSED.
    return out


def _opaque_next_blk(page: bytes, special: int) -> tuple[int, int]:
    """IvfflatPageOpaque at end of page: nextblkno, page_id."""
    if special + 8 > len(page):
        raise ValueError("special out of range")
    nextblk, u16_unused, page_id = struct.unpack_from("<IHH", page, special)
    return nextblk, page_id


def _parse_meta(page: bytes) -> tuple[int, int]:
    """Metapage: IvfflatMetaPageData at offset 24. Returns (dimensions, lists)."""
    magic, version, dimensions, lists = struct.unpack_from("<IIHH", page, 24)
    if magic != IVFFLAT_MAGIC:
        raise ValueError(f"unexpected ivfflat magic {magic:#x}, expected {IVFFLAT_MAGIC:#x}")
    if version != 1:
        raise ValueError(f"unexpected ivfflat version {version}")
    return dimensions, lists


def _varlena_payload_size_4b(header_le: int) -> int:
    """
    PostgreSQL 4-byte varlena header: total byte length is in upper bits (VARSIZE_4B).
    Stored value is NOT raw byte length; e.g. 3080-byte Vector often has header word 12320
    because (12320 >> 2) == 3080.
    """
    # 1-byte-header varlena: low bit set (only for very small values; not used for large vectors).
    if (header_le & 0x01) == 0x01:
        return (header_le >> 1) & 0x7F
    return (header_le >> 2) & 0x3FFFFFFF


def _extract_list_center_vector(item: bytes, expected_dim: int) -> list[float]:
    """
    IvfflatListData: startPage, insertPage (2x BlockNumber), then embedded pgvector Vector.
    Vector.vl_len_ is a PostgreSQL varlena header (see _varlena_payload_size_4b).
    Optional padding before Vector: try aligned offsets 8, 12, 16, ...
    """
    for vec_off in range(8, min(len(item), 48), 4):
        b = item[vec_off:]
        if len(b) < 8:
            break
        raw_header, dim, _unused = struct.unpack_from("<Ihh", b, 0)
        payload_len = _varlena_payload_size_4b(raw_header)
        need = 8 + 4 * dim
        if dim != expected_dim or payload_len != need or need < 8:
            continue
        if len(b) < need:
            continue
        return list(struct.unpack_from("<" + "f" * dim, b, 8))
    raise ValueError(
        f"no valid pgvector header in list item (len={len(item)}, expected_dim={expected_dim})"
    )


def _count_live_tuples_on_page(page: bytes) -> int:
    lower, _upper, special = _page_header(page)
    slots = _read_item_ids(page, lower)
    n = 0
    for off, flags, length in slots:
        if flags != LP_NORMAL:
            continue
        if off == 0 or length == 0:
            continue
        # Tuple must lie between end of line pointers and special.
        if not (lower <= off < special):
            continue
        if off + length > special:
            continue
        n += 1
    return n


def _walk_list_tuple_count(page_fetch: Callable[[int], bytes], start_blk: int) -> int:
    if start_blk == INVALID_BLOCK:
        return 0
    total = 0
    blk = start_blk
    seen: set[int] = set()
    while blk != INVALID_BLOCK:
        if blk in seen:
            raise RuntimeError(f"cycle in list page chain at block {blk}")
        seen.add(blk)
        page = page_fetch(blk)
        _lower, _upper, special = _page_header(page)
        _next, page_id = _opaque_next_blk(page, special)
        if page_id != IVFFLAT_PAGE_ID:
            raise ValueError(f"unexpected page_id {page_id:#x} at block {blk}")
        total += _count_live_tuples_on_page(page)
        blk = _next
    return total


def _load_ivfflat_indexes(database: str) -> list[dict[str, str]]:
    sql = """
SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname) AS idx_reg,
       quote_ident(nr.nspname) || '.' || quote_ident(r.relname) AS tbl_reg,
       c.relname AS idx_name,
       r.relname AS tbl_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_index i ON i.indexrelid = c.oid
JOIN pg_class r ON r.oid = i.indrelid
JOIN pg_namespace nr ON nr.oid = r.relnamespace
JOIN pg_am a ON a.oid = c.relam
WHERE a.amname = 'ivfflat'
  AND nr.nspname = 'public'
  AND r.relname IN ('part', 'partsupp')
ORDER BY r.relname, c.relname;
"""
    rows = []
    for line in _psql_sql(sql, database).splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != 4:
            continue
        rows.append(
            {
                "idx_reg": parts[0],
                "tbl_reg": parts[1],
                "idx_name": parts[2],
                "tbl_name": parts[3],
            }
        )
    return rows


def _indexed_vector_type(database: str, idx_reg: str) -> str:
    sql = f"""
SELECT t.typname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
JOIN pg_type t ON t.oid = a.atttypid
WHERE i.indexrelid = '{idx_reg}'::regclass;
"""
    out = _psql_sql(sql, database).strip()
    if not out:
        raise RuntimeError(f"could not resolve indexed type for {idx_reg}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Dump IVFFlat list centers and tuple counts (tpch10 part/partsupp).")
    ap.add_argument("--database", default=os.environ.get("PGDATABASE", "tpch10"))
    ap.add_argument(
        "--center",
        choices=("full", "first16", "none"),
        default="full",
        help="How to emit center vectors (default: full).",
    )
    ap.add_argument("--format", choices=("json", "jsonl"), default="json")
    args = ap.parse_args()

    # Probe pageinspect (needs CREATE EXTENSION pageinspect; and sufficient privilege).
    try:
        _psql_sql(
            "SELECT encode(get_raw_page('pg_catalog.pg_class'::text, 'main'::text, 0::int), 'hex');",
            args.database,
        )
    except RuntimeError:
        print(
            "ERROR: get_raw_page failed. Install/allow pageinspect, e.g.\n"
            "  CREATE EXTENSION IF NOT EXISTS pageinspect;\n"
            "(requires sufficient privileges.)",
            file=sys.stderr,
        )
        return 1

    indexes = _load_ivfflat_indexes(args.database)
    if not indexes:
        print("No ivfflat indexes on public.part / public.partsupp.", file=sys.stderr)
        return 1

    results: list[dict[str, Any]] = []

    for spec in indexes:
        idx_reg = spec["idx_reg"]
        typ = _indexed_vector_type(args.database, idx_reg)
        if typ != "vector":
            print(f"Skip {idx_reg}: indexed type is {typ!r} (only vector is supported).", file=sys.stderr)
            continue

        def fetch_blk(blk: int) -> bytes:
            # pageinspect exposes get_raw_page(text, text, int/bigint), not (regclass, ...).
            safe = idx_reg.replace("'", "''")
            hexed = _psql_sql(
                f"SELECT encode(get_raw_page('{safe}'::text, 'main'::text, {blk}::int), 'hex');",
                args.database,
            ).strip()
            if not re.fullmatch(r"[0-9a-fA-F]+", hexed) or len(hexed) % 2 != 0:
                raise RuntimeError(f"bad hex for block {blk}: {hexed[:80]!r}")
            return bytes.fromhex(hexed)

        page0 = fetch_blk(0)
        dimensions, nlist = _parse_meta(page0)

        lists_out: list[dict[str, Any]] = []
        list_id = 0
        next_list_page = 1
        seen_lp: set[int] = set()
        while next_list_page != INVALID_BLOCK:
            if next_list_page in seen_lp:
                raise RuntimeError("cycle in list-header page chain")
            seen_lp.add(next_list_page)
            lpage = fetch_blk(next_list_page)
            lower, _upper, special = _page_header(lpage)
            lp_next, page_id = _opaque_next_blk(lpage, special)
            if page_id != IVFFLAT_PAGE_ID:
                raise RuntimeError(f"list page {next_list_page}: bad page_id {page_id:#x}")

            slots = _read_item_ids(lpage, lower)
            for _, (off, flags, length) in enumerate(slots):
                if flags != LP_NORMAL or off == 0 or length == 0:
                    continue
                if not (lower <= off < special) or off + length > special:
                    continue
                item = lpage[off : off + length]
                if len(item) < 8:
                    continue
                start_page, _insert_page = struct.unpack_from("<II", item, 0)
                center = _extract_list_center_vector(item, dimensions)
                nvec = _walk_list_tuple_count(fetch_blk, start_page)
                rec: dict[str, Any] = {
                    "list_id": list_id,
                    "start_block": start_page if start_page != INVALID_BLOCK else None,
                    "vector_count": nvec,
                }
                if args.center == "full":
                    rec["center"] = center
                elif args.center == "first16":
                    rec["center_prefix16"] = center[:16]
                lists_out.append(rec)
                list_id += 1

            next_list_page = lp_next

        if list_id != nlist:
            print(
                f"[warn] {idx_reg}: parsed {list_id} lists, metapage nlist={nlist}",
                file=sys.stderr,
            )

        results.append(
            {
                "index": spec["idx_name"],
                "index_regclass": idx_reg,
                "table": spec["tbl_name"],
                "table_regclass": spec["tbl_reg"],
                "dimensions": dimensions,
                "nlist": nlist,
                "lists": lists_out,
            }
        )

    if args.format == "json":
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        for r in results:
            print(json.dumps(r, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
