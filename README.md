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

已完成(全部带测试,workspace 242 个测试):

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
- **代价优化器与统计信息(`sql::optimizer`,`server/tests/optimizer.rs`)**:
  - **表统计**(`common::stats`):row_count 精确增量(INSERT/DELETE 记账)、
    列 NDV / NULL 计数 / min-max 边界;ANALYZE [TABLE] 全量(>100 万行抽样)重建;
    规划期发现缺失/过期自动 ANALYZE 并持久化(重启恢复)
  - **选择性估算**:等值 1/NDV、范围按 min/max 线性插值、LIKE 启发式、
    IS NULL 按 NULL 分数;连接基数 = 积 × 1/max(键 NDV)
  - **连接顺序枚举**:≤10 表子集 DP(代价 = 读取 + 物化侧哈希表 + 输出),
    每步选小侧物化(必要时交换 probe/build 并投影还原列序);>10 表贪心;
    LEFT JOIN 为重排屏障(语义正确性优先)
  - **EXPLAIN <SELECT>**:统计驱动的计划树(扫描估行/下推过滤/连接键/物化侧)
  - 顺带修正:LEFT JOIN 空产侧谓词此前被错误下推(会改变 NULL 补齐语义),
    统一路由后按"位置 ≥ 自身的 LEFT 步骤挡住下推"处理
- **表达式 JIT(`exec::expr_compile`)**:
  - Expr 树 → **扁平字节码 + SSA 寄存器程序**,执行期一次顺序循环(无递归、
    无分发树);接入 FilterExec / ProjectExec(每查询每表达式编译一次)
  - **编译期三板斧**:常量折叠(`(1+2)*3` → 9)、代数化简(`x+0 → x`,
    严格 SQL 三值逻辑 —— `x*0 ↛ 0` 被明确禁止,NULL 传播不破坏)、
    公共子表达式消除(按子树文本键复用寄存器)
  - **单次使用移动**:寄存器消费计数,单次使用按移动取用(免整列拷贝)
  - SF0.1:合计 651ms → **590ms**(叠加上一轮累计 -40%)
- **投影下推(解码剪枝)**:planner 收集全查询引用列(投影/排序/WHERE/
  JOIN 键与残余/分组/聚合参数/HAVING)换算每表局部 needed 位图;
  `decode_row_partial` 按列跳过字节(编码自描述,游标安全推进)——
  未引用列(尤其 Utf8/Binary)零分配;SF0.1 合计 590 → **511ms**(-13%)
- **类型化哈希键 + Top-N 排序**:
  - HashJoin:全部连接键为单列 Int64 时自动切换 **HashMap<Vec<i64>> 快速通道**
    (直取类型化列数组,免 Vec<ScalarValue> 逐行装箱;NULL 键经位图判空);
    非整型/表达式键走通用路径
  - **Top-N 排序**:`ORDER BY ... LIMIT k(无 OFFSET)` 时 SortExec 切换为
    流式 Top-N(键行随批累积、每 4k 行裁剪一次,最终仅排序 k 行),
    planner 把 LIMIT 提示传入排序算子
  - SF0.1:合计 590ms → **543ms**(Q11 -15%、Q18 -14%、Q19 -12%)
  - 说明:这是**表达式级 JIT**(字节码 + 解释循环,Hyper/Impala 光谱中的
    折中层);LLVM/Cranelift 机器码与全流水线 codegen 见路线图
- **谓词下推 + AP 列存路径 + 并行执行(读写路径性能优化)**:
  - **谓词下推**:扫描谓词随解码过滤(`scan_table_filtered`),未命中行不进
    物化 —— JOIN build 侧、聚合输入、内存占用同步缩小;
  - **AP 列存路径**:标量表(Boolean/Int64/Float64/Utf8/Date)自动提交读
    走列存副本(整列向量,免逐行解码);列存副本补齐 **null 位图**与
    **Date 列**支持,与行存 NULL 语义一致;门控:事务内读回退行存
    (副本只镜像已提交状态,保证读己之写)、LOB/数组/向量列回退;
    **重启从 LSM 全量重建副本**(空副本不再被误当可信);
  - **并行框架**(`exec::parallel`):`scope_map` 作用域线程分片(保序拼接),
    应用于 LSM 解码过滤与 HashJoin 探测行组装;`engine.parallelism` 配置
    (默认 CPU 核数,上限 8);
  - SF0.1 基准:11 条合计 976ms → **651ms**(-33%),Q6 -60%、Q10 -62%。
- **内存控制(30 GiB 上限)**:
  - 配额树根默认限额提升到 **30 GiB**(`memory.global_limit_bytes`,0 = 不限);
    memtable / arena / chunk / 哈希表 / 物化行集全在同一棵树上记账
  - 补齐记账缺口:SeqScan 物化行集、HashJoin 构建哈希表 + 输出缓冲、
    引擎查询输出行 —— 超任何一层限额即报 `MemoryLimitExceeded`(SQLSTATE 53200)
  - Q19 根因修复:WHERE 为 OR-of-AND 且每个分支都含同一跨表等值对时,
    提取该对作为哈希键(完整 OR 仍作 residual 强制)—— 此前这类查询会退化成
    笛卡尔积全物化(12 亿行级别),正是内存爆掉的来源
- **表达式补全(CASE / IN / 非相关子查询,`server/tests/expressions.rs`)**:
  - **CASE WHEN**(搜索式):投影、WHERE、`SUM(CASE WHEN ...)` 聚合内均可
    (Q7/Q9/Q12/Q14 形状);结果类型统一(任一分支 Float64 → Int64 提升);
    条件 null 按未命中;BETWEEN 改写为范围比较
  - **IN 值列表 / NOT IN**:执行为哈希集合成员判定(非 OR 展开)
  - **非相关子查询**(计划期执行物化):标量 `(SELECT ...)`、
    `IN (SELECT ...)`、`[NOT] EXISTS (...)`;IN 子查询内部可含 GROUP BY + HAVING
    (Q18 形状);相关引用(引用外层别名)明确报错
  - 顺带修正存量 bug:HAVING 引入投影中没有的聚合(如 `HAVING SUM(...) > 子查询`)
    会因 agg 输出 schema 计算过早而失败 —— HAVING 改写提前到 AggHashExec 构建前

路线图(按依赖排序):

- **M1 事务剩余**:写冲突检测(first-committer-wins)、语句级保存点、
  隔离级别提升(快照隔离)
- **M3 HTAP 落地**:AP 扫描走列存副本(行/列双跑一致性对照);
  列存 segment 文件(字典/RLE 编码)+ 后台 compaction
- **M2/M4/M5**:RIGHT/FULL JOIN、**相关子查询**(EXISTS/IN 引用外层列)与
  FROM 派生表、CREATE VIEW、COUNT(DISTINCT)、TPC-H 数据加载(dbgen tbl 导入)、
  extended query protocol(psql \d)、块缓存、
  分级压缩、manifest 落盘、流式归并扫描、系统视图、DISTINCT、
  直方图/多列统计、连接代价标定(benchmark)

## 已知简化(读代码前先看)

- 扫描是"各来源批内物化 + 合并排序",非流式 k 路归并(数据量大前够用)。
- 压缩是全量合并(单 L1 文件),无分级限流。
- manifest 不落盘,靠 WAL + SST 文件名重建(单进程假设)。
- And/Or 的 null 按 false 处理(三值逻辑 TODO)。
- extended query protocol 未接(psql 交互查询走 simple 协议不受影响)。
- 事务无写冲突检测(后提交者覆盖,RC 语义下允许);无保存点;DDL 不能在事务内。
- ORDER BY / 聚合为全量物化后排序(无外部排序/top-N 堆优化)。
- HashJoin 构建侧整批物化;代价模型未标定(权重为启发值);统计为
  单列 NDV/min-max(无直方图、无相关性);NDV 写路径不增量(靠 ANALYZE)。
- DECIMAL 收敛为 Float64(TPC-H 数值列够用,精确小数见路线图)。
- CASE WHEN 仅搜索式(`CASE WHEN cond THEN ...`),简单式 `CASE <operand>` 拒绝;
  子查询为**非相关**(计划期执行一次);EXISTS 不提前终止于首条匹配但仍按批
  拉取(实现为"取到首个非空批即停")。
- IN 的成员判定按标量精确相等(Int64 与 Float64 不跨类型匹配);IN 子查询结果
  整体物化(大结果集走半连接见路线图)。

## 开发

```bash
cargo test          # 全部测试
cargo test -p medb-storage   # 单 crate
cargo check --workspace --all-targets
```
