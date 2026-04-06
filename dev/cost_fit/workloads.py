#!/usr/bin/env python3
"""Generated (tag, sql) workloads: at least N distinct queries per operator family."""

from __future__ import annotations


def _uniq(ws: list) -> list:
    seen = set()
    out = []
    for tag, sql in ws:
        if sql in seen:
            continue
        seen.add(sql)
        out.append((tag, sql))
    return out


# --- Seq Scan: vary mods, dates, limits ---
def scan_workloads(target: int = 55) -> list:
    ws: list = [
        ("region_all", "SELECT * FROM region"),
        ("nation_all", "SELECT * FROM nation"),
    ]
    mods = [2, 3, 5, 7, 11, 13, 17]
    for m in mods:
        for r in range(min(4, m)):
            ws.append((f"sup_m{m}_r{r}", f"SELECT s_suppkey, s_name FROM supplier WHERE s_suppkey % {m} = {r}"))
    for m in mods:
        for r in range(min(4, m)):
            ws.append((f"cust_m{m}_r{r}", f"SELECT c_custkey, c_name FROM customer WHERE c_custkey % {m} = {r}"))
    for m in mods:
        for r in range(min(4, m)):
            ws.append((f"ord_m{m}_r{r}", f"SELECT o_orderkey, o_orderdate FROM orders WHERE o_orderkey % {m} = {r}"))
    for m in [3, 5, 7, 10, 13, 17, 19, 23, 29]:
        for r in (0, 1, 2):
            ws.append(
                (f"li_m{m}_r{r}", f"SELECT l_orderkey, l_quantity FROM lineitem WHERE l_orderkey % {m} = {r}")
            )
    for d in (
        "1992-03-01",
        "1993-01-15",
        "1994-06-01",
        "1995-01-01",
        "1996-08-20",
        "1997-11-11",
        "1998-12-01",
    ):
        ws.append((f"li_ship_{d}", f"SELECT l_shipdate, l_discount FROM lineitem WHERE l_shipdate < DATE '{d}'"))
    for m in [5, 11, 17, 23, 29, 31]:
        for r in (0, 1):
            ws.append((f"part_m{m}_r{r}", f"SELECT p_partkey, p_type FROM part WHERE p_partkey % {m} = {r}"))
    for m in [5, 9, 13, 17, 19]:
        for r in (0, 2, 4):
            ws.append(
                (f"ps_m{m}_r{r}", f"SELECT ps_partkey, ps_availqty FROM partsupp WHERE ps_partkey % {m} = {r}")
            )
    for lim in (2000, 10000, 80000, 400000, 800000):
        ws.append((f"li_lim_{lim}", f"SELECT l_orderkey, l_partkey FROM lineitem WHERE l_quantity > 0 LIMIT {lim}"))
    for lim in (5000, 25000, 120000):
        ws.append((f"ord_lim_{lim}", f"SELECT o_orderkey FROM orders WHERE o_orderstatus = 'F' LIMIT {lim}"))
    ws = _uniq(ws)
    return ws[:target] if len(ws) >= target else ws


# --- Index-friendly predicates (use with enable_seqscan=off in session) ---
def index_scan_workloads(target: int = 55) -> list:
    ws: list = []
    # lineitem shipdate windows
    pairs = [
        ("1992-01-01", "1992-12-31"),
        ("1993-01-01", "1993-03-31"),
        ("1993-04-01", "1993-09-30"),
        ("1994-01-01", "1994-06-30"),
        ("1995-01-01", "1995-12-31"),
        ("1996-02-01", "1996-08-31"),
        ("1997-01-01", "1997-12-31"),
    ]
    lims = (50000, 150000, 400000, 800000)
    for i, (a, b) in enumerate(pairs):
        for j, lim in enumerate(lims):
            ws.append(
                (
                    f"idx_li_{i}_{lim}",
                    f"SELECT l_orderkey FROM lineitem WHERE l_shipdate BETWEEN DATE '{a}' AND DATE '{b}' LIMIT {lim}",
                )
            )
    # orders orderdate + custkey
    for a, b in (
        ("1992-06-01", "1993-06-01"),
        ("1993-01-01", "1994-01-01"),
        ("1994-06-01", "1995-06-01"),
        ("1995-01-01", "1996-12-31"),
        ("1996-01-01", "1997-06-30"),
    ):
        ws.append(
            (
                f"idx_ord_{a}",
                f"SELECT o_orderkey FROM orders WHERE o_orderdate BETWEEN DATE '{a}' AND DATE '{b}'",
            )
        )
    for pct in (20, 50, 100, 200, 300, 500, 1000, 2000):
        ws.append(
            (
                f"idx_ord_cust_{pct}",
                f"SELECT o_orderkey FROM orders WHERE o_custkey % 256 = {pct % 256} LIMIT 40000",
            )
        )
    for lo, hi in ((1, 40000), (10000, 90000), (50000, 200000), (1, 120000), (3000, 80000)):
        ws.append((f"idx_part_{lo}_{hi}", f"SELECT p_partkey FROM part WHERE p_partkey BETWEEN {lo} AND {hi}"))
    for lo, hi in ((1, 8000), (5000, 25000), (10000, 40000), (1, 50000)):
        ws.append(
            (
                f"idx_ps_{lo}_{hi}",
                f"SELECT ps_partkey FROM partsupp WHERE ps_partkey BETWEEN {lo} AND {hi} LIMIT 150000",
            )
        )
    for nk in (0, 1, 2, 3, 5, 7, 12, 15, 18, 21):
        ws.append((f"idx_cust_nat_{nk}", f"SELECT c_custkey FROM customer WHERE c_nationkey = {nk}"))
    ws = _uniq(ws)
    if len(ws) < target:
        return ws
    return ws[:target]


def sort_workloads(target: int = 55) -> list:
    # ORDER BY on indexed columns (e.g. orders.o_orderdate vs idx_orders_orderdate) must be
    # collected with index scans disabled (see 05_collect_sort.sort_explain_prefix) or the
    # plan has no Sort node.
    ws = []
    limits = [4000, 12000, 25000, 45000, 70000, 95000, 140000, 220000]
    specs = [
        ("lineitem", "l_extendedprice DESC", "l_orderkey", "lip"),
        ("lineitem", "l_quantity NULLS LAST", "l_partkey", "liq"),
        ("lineitem", "l_discount", "l_orderkey", "lid"),
        ("orders", "o_totalprice DESC", "o_orderkey", "ot"),
        ("orders", "o_orderdate", "o_orderkey", "od"),
        ("part", "p_name", "p_partkey", "pn"),
        ("customer", "c_acctbal DESC", "c_custkey", "ca"),
        ("supplier", "s_acctbal", "s_suppkey", "sa"),
    ]
    for lim in limits:
        for table, ob, extra, pfx in specs:
            ws.append(
                (
                    f"sort_{pfx}_{lim}",
                    f"SELECT {extra}, {ob.split()[0]} FROM {table} ORDER BY {ob} LIMIT {lim}",
                )
            )
    ws = _uniq(ws)
    return ws[:target] if len(ws) >= target else ws


def hashjoin_workloads(target: int = 55) -> list:
    ws = []
    mods = (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 37, 41, 43, 47)
    for m in mods:
        ws.append(
            (
                f"hj_ol_m{m}",
                f"SELECT COUNT(*) FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey "
                f"WHERE o.o_orderkey % {m} = 0 AND l.l_orderkey % {m} = 0",
            )
        )
    for hi in (3000, 8000, 15000, 50000, 100000, 350000, 600000, 900000, 1400000, 2200000):
        ws.append(
            (
                f"hj_ol_band_{hi}",
                f"SELECT COUNT(*) FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey "
                f"WHERE o.o_orderkey BETWEEN 1 AND {hi}",
            )
        )
    for m in (2, 3, 4, 5, 7, 9, 11, 13, 17, 19, 23):
        ws.append(
            (
                f"hj_co_m{m}",
                f"SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_custkey % {m} = 0",
            )
        )
    for lo, hi in ((1, 8000), (1, 35000), (1000, 90000), (5000, 70000), (1, 180000)):
        ws.append(
            (
                f"hj_ps_{lo}_{hi}",
                f"SELECT COUNT(*) FROM part p JOIN partsupp ps ON p.p_partkey = ps.ps_partkey "
                f"WHERE p.p_partkey BETWEEN {lo} AND {hi}",
            )
        )
    for m in (1, 2, 3, 5, 7, 11, 13):
        ws.append(
            (
                f"hj_sn_{m}",
                f"SELECT COUNT(*) FROM supplier s JOIN nation n ON s.s_nationkey = "
                f"n.n_nationkey WHERE s.s_suppkey % {m} = 0",
            )
        )
    ws.append(("hj_nr", "SELECT COUNT(*) FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey"))
    ws.append(
        (
            "hj_cn",
            "SELECT COUNT(*) FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE n.n_nationkey < 15",
        )
    )
    for nk in (0, 3, 8, 12):
        ws.append(
            (
                f"hj_lip_{nk}",
                "SELECT COUNT(*) FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey "
                f"WHERE p.p_partkey % 7 = {nk % 7} AND l.l_orderkey % 11 = 0",
            )
        )
    ws = _uniq(ws)
    return ws[:target] if len(ws) >= target else ws


def mergejoin_workloads(target: int = 55) -> list:
    ws = []
    for hi in (
        3000,
        6000,
        12000,
        15000,
        40000,
        90000,
        200000,
        450000,
        900000,
        1600000,
        2100000,
    ):
        ws.append(
            (
                f"mj_ol_{hi}",
                f"SELECT COUNT(*) FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey "
                f"WHERE o.o_orderkey BETWEEN 1 AND {hi}",
            )
        )
    for m in (2, 3, 4, 5, 7, 9, 11, 13, 16, 17, 19, 23, 29):
        ws.append(
            (
                f"mj_co_{m}",
                f"SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey WHERE c.c_custkey % {m} = 0",
            )
        )
    for lo, hi in (
        (1, 50000),
        (1, 150000),
        (5000, 90000),
        (1, 200000),
        (2000, 60000),
        (1, 80000),
        (10000, 120000),
        (30000, 180000),
    ):
        ws.append(
            (
                f"mj_ps_{lo}_{hi}",
                f"SELECT COUNT(*) FROM part p JOIN partsupp ps ON p.p_partkey = ps.ps_partkey "
                f"WHERE p.p_partkey BETWEEN {lo} AND {hi}",
            )
        )
    for m in list(range(1, 30, 2)) + [31, 37, 41]:
        ws.append(
            (
                f"mj_sn_{m}",
                f"SELECT COUNT(*) FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey "
                f"WHERE s.s_suppkey % {m} = 0",
            )
        )
    ws.append(("mj_nr", "SELECT COUNT(*) FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey"))
    ws.append(("mj_cn", "SELECT COUNT(*) FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey"))
    for k in (1, 4, 9, 16):
        ws.append(
            (
                f"mj_lip_{k}",
                "SELECT COUNT(*) FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey "
                f"WHERE p.p_partkey % 13 = {k % 13} AND l.l_quantity > 0",
            )
        )
    for lo, hi in ((1, 400000), (50000, 500000), (1, 1000000)):
        ws.append(
            (
                f"mj_oc_{lo}_{hi}",
                f"SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey "
                f"WHERE c.c_custkey BETWEEN {lo} AND {hi}",
            )
        )
    ws = _uniq(ws)
    return ws[:target] if len(ws) >= target else ws


def agg_workloads(target: int = 55) -> list:
    ws = [
        ("agg_li_rf", "SELECT l_returnflag, count(*) FROM lineitem GROUP BY l_returnflag"),
        ("agg_li_ls", "SELECT l_linestatus, sum(l_quantity) FROM lineitem GROUP BY l_linestatus"),
        ("agg_li_sm", "SELECT l_shipmode, avg(l_extendedprice) FROM lineitem GROUP BY l_shipmode"),
        ("agg_li_si", "SELECT l_shipinstruct, count(*) FROM lineitem GROUP BY l_shipinstruct"),
        ("agg_li_rf_ls", "SELECT l_returnflag, l_linestatus, sum(l_quantity) FROM lineitem GROUP BY l_returnflag, l_linestatus"),
        ("agg_ord_st", "SELECT o_orderstatus, count(*) FROM orders GROUP BY o_orderstatus"),
        ("agg_ord_pr", "SELECT o_orderpriority, count(*) FROM orders GROUP BY o_orderpriority"),
        ("agg_ord_date_y", "SELECT date_trunc('year', o_orderdate), count(*) FROM orders GROUP BY 1"),
        ("agg_part_ty", "SELECT p_type, count(*) FROM part GROUP BY p_type"),
        ("agg_part_mf", "SELECT p_mfgr, avg(p_retailprice) FROM part GROUP BY p_mfgr"),
        ("agg_part_cont", "SELECT p_container, count(*) FROM part GROUP BY p_container"),
        ("agg_cust_seg", "SELECT c_mktsegment, count(*) FROM customer GROUP BY c_mktsegment"),
        ("agg_cust_nat", "SELECT c_nationkey, sum(c_acctbal) FROM customer GROUP BY c_nationkey"),
        ("agg_sup_nat", "SELECT s_nationkey, count(*) FROM supplier GROUP BY s_nationkey"),
    ]
    # more lineitem single-column + HAVING
    for m in (2, 3, 5, 7, 11):
        ws.append(
            (
                f"agg_li_tax_{m}",
                f"SELECT l_tax::text, count(*) FROM lineitem WHERE l_orderkey % {m} = 0 GROUP BY l_tax",
            )
        )
    for m in (13, 17):
        ws.append(
            (
                f"agg_li_disc_{m}",
                f"SELECT l_discount, count(*) FROM lineitem WHERE l_partkey % {m} = 0 GROUP BY l_discount",
            )
        )
    for d in ("1994-01-01", "1995-06-01", "1996-01-01"):
        ws.append(
            (
                f"agg_li_ship_{d}",
                f"SELECT l_shipmode, count(*) FROM lineitem WHERE l_shipdate < DATE '{d}' GROUP BY l_shipmode",
            )
        )
    for m in (3, 5, 7):
        ws.append(
            (
                f"agg_ord_clerk_{m}",
                f"SELECT o_clerk, count(*) FROM orders WHERE o_orderkey % {m} = 0 GROUP BY o_clerk",
            )
        )
    ws = _uniq(ws)
    if len(ws) < target:
        # pad with mod variations
        extra = []
        for k in range(200):
            if len(ws) + len(extra) >= target:
                break
            extra.append(
                (
                    f"agg_pad_li_{k}",
                    f"SELECT l_returnflag, count(*) FROM lineitem WHERE l_orderkey % 97 = {k % 97} GROUP BY l_returnflag",
                )
            )
        ws = _uniq(ws + extra)
    return ws[:target] if len(ws) >= target else ws
