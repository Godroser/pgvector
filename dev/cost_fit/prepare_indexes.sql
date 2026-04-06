-- Optional indexes for Index Scan / join experiments (tpch10 public schema)
CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON lineitem (l_shipdate);
CREATE INDEX IF NOT EXISTS idx_lineitem_partkey ON lineitem (l_partkey);
CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON orders (o_orderdate);
CREATE INDEX IF NOT EXISTS idx_orders_custkey ON orders (o_custkey);
CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON customer (c_nationkey);
CREATE INDEX IF NOT EXISTS idx_partsupp_partkey ON partsupp (ps_partkey);
CREATE INDEX IF NOT EXISTS idx_part_partkey ON part (p_partkey);
ANALYZE lineitem;
ANALYZE orders;
ANALYZE customer;
ANALYZE partsupp;
ANALYZE part;
