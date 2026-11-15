#!/bin/bash
# TPC-H 覆盖率检查:逐条跑未验证的查询,报告 支持/不支持 + 报错首行
P="E:/work/tools/pgsql/bin/psql.exe"
ARGS="-h 127.0.0.1 -p 55903 -U medb -d medb"

check() {
  local name="$1"; local sql="$2"
  local err
  err=$($P $ARGS -c "$sql" 2>&1 | grep -m1 -iE "^ERROR|panicked" )
  if [ -z "$err" ]; then
    echo "PASS  $name"
  else
    echo "FAIL  $name -> $err"
  fi
}

# Q2: 相关标量子查询(MIN)+ ORDER/LIMIT
check "Q2  相关标量子查询" "SELECT s_acctbal, s_name, p_partkey FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND p_size = 15 AND p_type LIKE '%BRASS%' AND s_acctbal = (SELECT MIN(s_acctbal) FROM supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND p_partkey = ps_partkey) ORDER BY s_acctbal, s_name, p_partkey LIMIT 100"

# Q4: EXISTS 相关子查询
check "Q4  EXISTS 相关子查询" "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= DATE '1995-01-01' AND o_orderdate < DATE '1995-04-01' AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority"

# Q8: 7 表连接 + 聚合相除 + EXTRACT + CASE(region 不取别名,限定名按表名解析)
check "Q8  聚合相除 + EXTRACT" "SELECT EXTRACT(YEAR FROM o_orderdate) AS all_year, 100.00 * SUM(CASE WHEN n1.n_name = 'BRAZIL' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS mkt_share FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_name = 'BRAZIL' AND s_nationkey = n2.n_nationkey AND n2.n_regionkey = r_regionkey AND o_orderdate >= DATE '1995-01-01' AND o_orderdate < DATE '1996-12-31' GROUP BY all_year ORDER BY all_year"

# Q9: LIKE '%..%' + EXTRACT + CASE
check "Q9  LIKE 模糊 + EXTRACT" "SELECT EXTRACT(YEAR FROM l_shipdate) AS l_year, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM part, supplier, lineitem, orders, customer, nation WHERE s_suppkey = l_suppkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND c_nationkey = n_nationkey AND n_name = 'GERMANY' AND p_type LIKE '%GREEN%' GROUP BY l_year ORDER BY l_year, revenue DESC"

# Q13: LEFT OUTER JOIN + NOT LIKE(ON 内);基准 orders 无 o_comment,
# 以 o_orderpriority NOT LIKE 保持同等语法形状(LEFT JOIN + ON 谓词 + COUNT)
check "Q13 LEFT JOIN + NOT LIKE" "SELECT c_name, COUNT(o_orderkey) AS n_orders FROM customer LEFT OUTER JOIN orders ON c_custkey = o_orderkey AND o_orderpriority NOT LIKE '5% LOW%' GROUP BY c_name ORDER BY n_orders DESC, c_name LIMIT 20"

# Q15: WITH CTE
check "Q15 WITH CTE" "WITH revenue AS (SELECT l_suppkey AS supplier_no, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01' GROUP BY l_suppkey) SELECT s_suppkey, s_name, total_revenue FROM revenue, supplier WHERE s_suppkey = supplier_no AND total_revenue = (SELECT MAX(total_revenue) FROM revenue) ORDER BY s_suppkey"

# Q16: COUNT(DISTINCT) + NOT IN + NOT LIKE
check "Q16 COUNT(DISTINCT)" "SELECT p_brand, p_type, p_size, COUNT(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size NOT IN (49, 14, 23, 45, 19, 3, 36, 9) GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size LIMIT 10"

# Q17: 相关标量子查询(AVG × 常量)
check "Q17 相关子查询 AVG" "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM part, lineitem WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)"

# Q20: 相关 IN 子查询(嵌套相关;标准原文含 l_orderkey = o_orderkey 连接)
check "Q20 相关 IN 子查询" "SELECT s_name, COUNT(*) AS num_supply FROM supplier, nation WHERE s_nationkey = n_nationkey AND n_name = 'GERMANY' AND s_suppkey IN (SELECT ps_suppkey FROM partsupp, part WHERE ps_partkey = p_partkey AND p_type LIKE '%GREEN%' AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_suppkey = ps_suppkey AND l_partkey = ps_partkey AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01')) GROUP BY s_name ORDER BY s_name, num_supply LIMIT 10"

# Q21: EXISTS + NOT EXISTS(多重相关)
check "Q21 EXISTS + NOT EXISTS" "SELECT s_name, COUNT(*) AS numwait FROM supplier, lineitem l1, orders, nation WHERE s_suppkey = l1.l_suppkey AND l1.l_orderkey = o_orderkey AND o_orderpriority = '1-URGENT' AND l1.l_receiptdate > l1.l_commitdate AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey AND l3.l_commitdate < l3.l_receiptdate) AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"

# Q22: FROM 派生表 + 非相关标量子查询
check "Q22 FROM 派生表" "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal FROM customer WHERE SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17') AND c_acctbal > (SELECT AVG(c_acctbal) FROM customer WHERE c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17'))) AS custorders GROUP BY cntrycode ORDER BY cntrycode"

echo "--- done ---"
