\echo Q1
EXPLAIN (ANALYZE, TIMING OFF)
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;
\echo Q3
EXPLAIN (ANALYZE, TIMING OFF)
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, l_orderkey LIMIT 10;
\echo Q5
EXPLAIN (ANALYZE, TIMING OFF)
SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1996-01-01' AND o_orderdate < DATE '1997-01-01' GROUP BY n_name ORDER BY revenue DESC;
\echo Q6
EXPLAIN (ANALYZE, TIMING OFF)
SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.02 AND 0.09 AND l_quantity < 24;
\echo Q7
EXPLAIN (ANALYZE, TIMING OFF)
SELECT n_name, SUM(CASE WHEN n_name = 'FRANCE' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS france_revenue, SUM(CASE WHEN n_name = 'GERMANY' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS germany_revenue FROM customer, orders, lineitem, supplier, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND (n_name = 'FRANCE' OR n_name = 'GERMANY') GROUP BY n_name ORDER BY n_name;
\echo Q10
EXPLAIN (ANALYZE, TIMING OFF)
SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_address, c_phone, c_comment, n_name ORDER BY revenue DESC LIMIT 20;
\echo Q11
EXPLAIN (ANALYZE, TIMING OFF)
SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') * 0.0001 ORDER BY value DESC;
\echo Q12
EXPLAIN (ANALYZE, TIMING OFF)
SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode;
\echo Q14
EXPLAIN (ANALYZE, TIMING OFF)
SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01';
\echo Q18
EXPLAIN (ANALYZE, TIMING OFF)
SELECT c_name, c_custkey, o_orderkey, o_totalprice, o_orderdate FROM customer, orders WHERE o_custkey = c_custkey AND o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 150) GROUP BY c_name, c_custkey, o_orderkey, o_totalprice, o_orderdate ORDER BY o_totalprice DESC, o_orderdate LIMIT 100;
\echo Q19
EXPLAIN (ANALYZE, TIMING OFF)
SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem, part WHERE (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 10 AND l_quantity <= 20 AND l_partkey = p_partkey AND p_brand = 'Brand#12' AND p_container IN ('SMALL CASE', 'SMALL BOX') AND p_size >= 1 AND p_size <= 5) OR (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 10 AND l_quantity <= 30 AND l_partkey = p_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX') AND p_size >= 1 AND p_size <= 10) OR (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 20 AND l_quantity <= 40 AND l_partkey = p_partkey AND p_brand = 'Brand#34' AND p_container IN ('LARGE CASE', 'LARGE PACK') AND p_size >= 1 AND p_size <= 15);
