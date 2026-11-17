#!/bin/bash
# DuckDB 对比:同一套 22 条查询(medb 的切分产物),逐条计时。
DDB="E:/work/tools/duckdb/duckdb.exe"
DB="E:/work/tools/duckdb/medb_tpch.duckdb"
SPLIT="/e/work/medb/crates/server/target/tpch_results/q"
printf "%-5s %12s\n" Q duckdb_ms
for f in "$SPLIT"/Q*.sql; do
  name=$(basename "$f" .sql)
  # 预热一次
  "$DDB" -c ".read $f" "$DB" >/dev/null 2>&1
  t=$({ echo '.timer on'; cat "$f"; } | "$DDB" "$DB" 2>&1 \
      | grep -m1 -oE "Run Time \(s\): real [0-9.]+" | grep -oE "[0-9.]+$")
  ms=$(awk -v s="$t" 'BEGIN{printf "%.1f", s*1000}')
  printf "%-5s %12s\n" "$name" "$ms"
done
