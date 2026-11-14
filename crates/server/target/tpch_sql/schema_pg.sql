CREATE TABLE region (r_regionkey BIGINT NOT NULL, r_name TEXT NOT NULL);
CREATE TABLE nation (n_nationkey BIGINT NOT NULL, n_name TEXT NOT NULL, n_regionkey BIGINT NOT NULL);
CREATE TABLE supplier (s_suppkey BIGINT NOT NULL, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey BIGINT NOT NULL, s_phone TEXT NOT NULL, s_acctbal DOUBLE PRECISION NOT NULL, s_comment TEXT NOT NULL);
CREATE TABLE part (p_partkey BIGINT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size BIGINT NOT NULL, p_container TEXT NOT NULL);
CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT NOT NULL, ps_supplycost DOUBLE PRECISION NOT NULL);
CREATE TABLE customer (c_custkey BIGINT NOT NULL, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey BIGINT NOT NULL, c_phone TEXT NOT NULL, c_acctbal DOUBLE PRECISION NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT NOT NULL);
CREATE TABLE orders (o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderdate DATE NOT NULL, o_shippriority BIGINT NOT NULL, o_totalprice DOUBLE PRECISION NOT NULL, o_orderpriority TEXT NOT NULL);
CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL, l_extendedprice DOUBLE PRECISION NOT NULL, l_discount DOUBLE PRECISION NOT NULL, l_tax DOUBLE PRECISION NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT NOT NULL);
