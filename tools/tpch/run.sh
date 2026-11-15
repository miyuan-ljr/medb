#!/bin/bash
# TPC-H 22 条全量运行:medb 与 PG 各跑一遍,落盘结果 + 耗时,逐条对拍。
# 用法: bash tools/tpch/run.sh
#   medb  : 127.0.0.1:55903 (medb_tpch 数据)
#   PG    : 127.0.0.1:5432  (pgdata_tpch 数据)
# 输出: crates/server/target/tpch_results/{medb,pg}/Q*.txt + summary.txt
PSQL="E:/work/tools/pgsql/bin/psql.exe"
M="-h 127.0.0.1 -p 55903 -U medb -d medb -A"
P="-h 127.0.0.1 -p 5432 -U postgres -d tpch -A"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/crates/server/target/tpch_results"
SPLIT="$OUT/q"
rm -rf "$OUT"; mkdir -p "$OUT/medb" "$OUT/pg" "$SPLIT"

# 按 "-- QN" 标记切分(标记行不落文件)
awk -v d="$SPLIT" '/^-- Q/{f=d"/"$2".sql"} !/^-- Q/ && !/^$/{if(f!="")print > f}' \
  "$ROOT/tools/tpch/queries.sql"

printf "%-5s %10s %10s %8s %8s  %s\n" Q medb_ms pg_ms rows_m rows_p match
for f in "$SPLIT"/Q*.sql; do
  name=$(basename "$f" .sql)
  # medb:预热一遍再计时(段解码缓存)
  $PSQL $M -f "$f" >/dev/null 2>&1
  mt=$({ echo '\timing on'; cat "$f"; } | $PSQL $M 2>&1 \
        | grep -m1 -oE "Time: [0-9.]+" | grep -oE "[0-9.]+")
  $PSQL $M -f "$f" > "$OUT/medb/$name.txt" 2>"$OUT/medb/$name.err"
  # PG:同样预热再计时
  $PSQL $P -f "$f" >/dev/null 2>&1
  pt=$({ echo '\timing on'; cat "$f"; } | $PSQL $P 2>&1 \
        | grep -m1 -oE "Time: [0-9.]+" | grep -oE "[0-9.]+")
  $PSQL $P -f "$f" > "$OUT/pg/$name.txt" 2>"$OUT/pg/$name.err"

  rm_=$(grep -vc '^$' "$OUT/medb/$name.txt" 2>/dev/null || echo ERR)
  rp_=$(grep -vc '^$' "$OUT/pg/$name.txt" 2>/dev/null || echo ERR)
  # 归一化对比:行排序;数值统一 12 位有效数字(两库 SUM 累加顺序不同会有 ~1e-12 相对尾数差)
  norm='{
    n = split($0, a, "|")
    for (i = 1; i <= n; i++)
      if (a[i] ~ /^-?[0-9]+\.[0-9]+$/) a[i] = sprintf("%.12g", a[i] + 0)
    print a[1]
    for (i = 2; i <= n; i++) print "|" a[i]
  }'
  awk "$norm" "$OUT/medb/$name.txt" | sort > "$OUT/medb/$name.norm"
  awk "$norm" "$OUT/pg/$name.txt" | sort > "$OUT/pg/$name.norm"
  if cmp -s "$OUT/medb/$name.norm" "$OUT/pg/$name.norm"; then
    match="OK"
  else
    match="DIFF"
    diff "$OUT/pg/$name.norm" "$OUT/medb/$name.norm" > "$OUT/$name.diff" 2>&1
  fi
  printf "%-5s %10s %10s %8s %8s  %s\n" "$name" "${mt:-ERR}" "${pt:-ERR}" "$rm_" "$rp_" "$match"
done | tee "$OUT/summary.txt"
echo "--- 结果与耗时明细: $OUT ---"
