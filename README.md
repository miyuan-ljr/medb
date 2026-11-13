# medb

轻量级 HTAP 数据库(单人开发,教学/实验导向)。

- **存储引擎**:LSM tree(行存主库)
- **执行引擎**:向量化(批式列存 + 类型化内核)
- **HTAP**:行存为 TP 主库,列存副本异步同步服务 AP 查询(钩子已就位)
- **客户端**:现成 psql / 任意 PostgreSQL 驱动(PG wire 协议)

## 快速开始

```bash
cargo run -- serve
# 另一个终端:
psql -h 127.0.0.1 -p 55432 -U postgres
postgres=# CREATE TABLE users (id BIGINT NOT NULL, name TEXT, score DOUBLE);
postgres=# INSERT INTO users VALUES (1, 'alice', 90.5), (2, 'bob', 75.0);
postgres=# SELECT name, score * 2 AS doubled FROM users WHERE id >= 2;
postgres=# DELETE FROM users WHERE id = 1;
# 重启 medb 后表和数据都还在(catalog 与数据均持久化于 LSM)
```

本机没有 psql 也没关系:`cargo test` 的协议/引擎集成测试覆盖了同一条链路
(原生 TCP 走 startup → Query → RowDescription/DataRow/CommandComplete → 错误恢复)。

## 架构

```text
                     psql / PG 驱动
                          │ PG wire (simple query)
                 ┌────────▼────────┐
                 │  server         │  会话/协议/错误映射(SQLSTATE)
                 ├─────────────────┤
                 │  sql            │  解析(sqlparser)→ 计划(骨架)
                 ├─────────────────┤
                 │  exec           │  向量化执行:Vector/DataChunk/算子
                 ├─────────────────┤
                 │  storage        │  LSM:memtable/WAL/SST/compaction
                 │  columnar       │  列存副本(HTAP AP 路径钩子)
                 ├─────────────────┤
                 │  memory         │  mimalloc + 配额树 + arena
                 ├─────────────────┤
                 │  common         │  类型/schema/编解码/错误/配置
                 └─────────────────┘
                 catalog(sql ↔ storage 元数据)
                 medb(bin 入口)
```

各 crate 说明:

| crate(lib 名) | 内容 |
| --- | --- |
| `medb-common`(common) | 错误、`DataType`/`ScalarValue`、schema、行编码、varint、配置 |
| `medb-memory`(memory) | 全局分配器(mimalloc,feature 可关)、`MemoryTracker` 配额树、`Arena` |
| `medb-storage`(storage) | 内部键编码、memtable(跳表)、WAL、SST(block/bloom/index)、压缩、恢复、列存副本 |
| `medb-exec`(exec) | `Vector`(null 位图)/`DataChunk`、表达式内核、`Executor` 拉取框架、算子 |
| `medb-catalog`(catalog) | 元数据 trait + 内存实现 |
| `medb-sql`(sql) | 解析 + 骨架计划器(无 FROM SELECT → 流水线) |
| `medb-server`(server) | pgwire 0.42 接入、查询执行、SQLSTATE 映射 |
| `medb`(bin) | CLI、tracing 初始化、启动 |

## 内存分配机制(三层)

1. **全局分配器**:mimalloc(`feature = "mimalloc"`,默认开;关掉回退系统分配器,便于对照 benchmark)。Windows/MSVC 下不选 jemalloc(构建困难)。
2. **配额树**(`memory::tracker`):`全局 → 会话 → 查询` 的层级限额。`try_reserve` 沿链记账,任一层超限即回滚并返回 `MemoryLimitExceeded`;`Reservation` RAII 归还。memtable 按 `write_buffer_bytes` 触发自动 flush;`DataChunk` 按近似大小在查询节点记账。
3. **Arena**(`memory::arena`):查询生命周期 bump 分配器,chunk 按需扩容、整块归还;chunk 也走配额树,统计与限额天然一致。

配置见 `config/medb.example.toml`(`global_limit_bytes` / `query_limit_bytes` / `arena_chunk_bytes` 等)。

## 存储格式速览

- **内部键**:`user_key ++ [!seq 8B BE ++ kind 1B]`;比较器先 memcmp user_key 再比 trailer(倒序),同键新版本天然在前。**不能**对原始字节 memcmp("ab" 与 "abc" 会被 trailer 干扰)。
- **WAL**:段文件 `{segment:020}.wal`,帧 `[len u32LE][crc32][payload]`;崩溃时尾部半帧/CRC 不符即截断。
- **SST**:`[data block*][bloom][index][footer 68B]`;块内 `varint klen|key|varint vlen|value` + CRC;footer 含 min/max seq 与 magic `LTB1`。文件名 `{id:020}.{level}.sst`,启动时由文件名重建清单(免 manifest 落盘,v1 简化)。
- **压缩**:L0 文件数达阈值 → L0+L1 全量合并为单个 L1(正确性优先;分级合并见路线图);
  **GC 水位感知**:seq ≤ 水位且被可见版本遮蔽的旧版本才回收(活跃快照保护)。

- **表数据键布局**:`row_key = table_id u64BE ++ row_id u64BE`(同表聚簇、扫描序 = 插入序);
  row_id 派生自全局序列,重启水位 = max(WAL, SST max_seq),永不复用。
  catalog 元数据持久化在保留表 0(键 = 表名,值 = serde_json 的 TableSchema),
  随 LSM 一起恢复。

- **事务(RC)**:写入暂存会话缓冲,COMMIT 一次落盘(单条 WAL 记录 = 恢复原子);
  读走 `read_ts` 快照(memtable/SST 点查与扫描均支持 seq ≤ ts 版本选择);
  语句级快照注册表推 GC 水位。

- **schema 多版本**:`TableSchema.version` 每次 ALTER 递增并持久化;
  行解码前缀兼容(旧行缺列补 NULL、新行多余列截断),跨版本读写无需立即迁移。

## 当前进度与路线图

已完成(全部带测试,workspace 187 个测试):

- 内存机制三层(mimalloc / 配额树 / arena)
- LSM 完整闭环:WAL 恢复、SST 读写、flush、压缩、墓碑、并发单写者、序列水位恢复、
  **MVCC 快照读(read_ts)与 GC 水位压缩**
- 向量化内核:类型化列 + null 位图、算术/比较/逻辑、filter/project/values/limit/seq_scan/
  **hash_agg(COUNT/SUM/AVG/MIN/MAX,含 null 语义与溢出检查)/sort(多键、NULLS FIRST/LAST)**
- **SQL → 存储全链路**:`CREATE TABLE` / `INSERT`(VALUES + **INSERT SELECT**)/
  `SELECT`(WHERE 列谓词 / 投影 / `t.col` / **GROUP BY + HAVING / 聚合表达式** /
  **ORDER BY(序号、别名、隐藏键列)** / LIMIT OFFSET)/ **UPDATE**(旧行语义赋值)/
  `DELETE` / `DROP TABLE` / **`ALTER TABLE ADD COLUMN`(schema 版本递增、行重写)**
- **事务(READ COMMITTED)**:BEGIN/COMMIT/ROLLBACK、会话写缓冲 + 快照 overlay
  (读己之写)、COMMIT 原子落盘、事务内 DDL 拒绝、多会话隔离测试
- PG 协议端到端:RowDescription/DataRow/CommandComplete/ErrorResponse、
  INSERT 0 n / UPDATE n / DELETE n / CREATE TABLE / BEGIN / COMMIT 命令标签
- 列存副本 trait + 内存实现(CDC upsert/delete/drop_table/scan_column)
- **TPC-H 基础(以 TPC-H 形状为验收标准,`server/tests/tpch.rs`)**:
  - **多表 FROM + JOIN 计划**:FROM 展开为有序关系步骤(逗号连接 = CROSS JOIN),
    每步 `SeqScan(下推过滤)→ HashJoin`;等值条件两侧各引一表 → 哈希键(probe 侧扁平下标、
    build 侧重基到表内),非等值/跨多表条件 → 匹配期 match_cond / 连接后 residual;
    WHERE 合取项按引用表路由(单表下推、跨表挂步骤、LEFT 保留侧延后)
  - **HashJoinExec**:inner + left outer;NULL 键不匹配;LEFT 未命中补 NULL,
    ON 非等值残余在匹配期判定(未过条件的左行仍 NULL 补齐)
  - **DATE 类型**:i32 天数(1970-01-01 起),`DATE 'YYYY-MM-DD'` 字面量、日期比较
    (含字符串字面量隐式解析)、`± INTERVAL 'n' DAY`、`EXTRACT(YEAR/MONTH)`、
    PG DATE 线协议类型
  - **LIKE / NOT LIKE**:带回溯的 `%`/`_` 匹配器
  - **DECIMAL/NUMERIC → Float64**(无精确小数,v1 简化)
  - 字符串/数值函数:UPPER/LOWER/LENGTH/TRIM/REVERSE/CONCAT/COALESCE/IFNULL/NULLIF/
    ABS/ROUND/FLOOR/CEIL/SQRT/MOD/POWER/**SUBSTRING**

路线图(按依赖排序):

- **M1 事务剩余**:写冲突检测(first-committer-wins)、语句级保存点、
  隔离级别提升(快照隔离)
- **M3 HTAP 落地**:AP 扫描走列存副本(行/列双跑一致性对照);
  列存 segment 文件(字典/RLE 编码)+ 后台 compaction
- **M2/M4/M5**:RIGHT/FULL JOIN、TPC-H 数据加载(dbgen tbl 导入)、
  extended query protocol(psql \d)、块缓存、
  分级压缩、manifest 落盘、流式归并扫描、系统视图、DISTINCT/子查询/CASE WHEN

## 已知简化(读代码前先看)

- 扫描是"各来源批内物化 + 合并排序",非流式 k 路归并(数据量大前够用)。
- 压缩是全量合并(单 L1 文件),无分级限流。
- manifest 不落盘,靠 WAL + SST 文件名重建(单进程假设)。
- And/Or 的 null 按 false 处理(三值逻辑 TODO)。
- extended query protocol 未接(psql 交互查询走 simple 协议不受影响)。
- 事务无写冲突检测(后提交者覆盖,RC 语义下允许);无保存点;DDL 不能在事务内。
- ORDER BY / 聚合为全量物化后排序(无外部排序/top-N 堆优化)。
- HashJoin 构建侧整批物化;无连接顺序优化(按 FROM 顺序左深);无统计信息。
- DECIMAL 收敛为 Float64(TPC-H 数值列够用,精确小数见路线图)。
- CASE WHEN 暂不支持(sqlparser 0.64 语句化模型,待单独支持)。

## 开发

```bash
cargo test          # 全部测试
cargo test -p medb-storage   # 单 crate
cargo check --workspace --all-targets
```
