# medb 设计说明

本文记录关键设计决策与不变式,供后续开发对照。README 是概览,这里是"为什么"。

## 1. 总体结构

```
sql(front) → exec(向量化) → storage(LSM 行存)
                ↑                    ↓ CDC(计划)
              memory(配额/arena)   columnar(列存副本,AP)
```

依赖方向固定为 common ← memory ← {exec, storage} ← sql/catalog ← server。
storage 不依赖 exec(列存用 `storage::columnar::ColumnVector`,exec 侧转换)。

## 2. 内存分配机制

### 2.1 全局层
- mimalloc:多线程扩展性好、Windows/MSVC 构建无障碍;`#[global_allocator]`
  在 `memory::global`,feature `mimalloc` 可关(回退系统分配器做对照实验)。
- 不做全局统计钩子(每分配一次原子计数开销大),统计在逻辑层(配额树)做。

### 2.2 配额树(MemoryTracker)
- 节点:`{name, limit: AtomicI64(-1=无限), used: AtomicI64, parent: Weak, children: Weak 列表}`。
  父子互持 Weak,子节点 drop 后快照自动剪枝。
- `try_reserve(bytes)`:自身→根逐层 `fetch_add` 并检查;超限回滚整链。
  用 fetch-then-check 而非 CAS:允许瞬时过冲,简单且足够(记账是软限额)。
- `Reservation`:RAII,持有整条强引用链,drop 时逐层减回;`resize` 支持
  扩容/缩容(arena 增 chunk、memtable 累计记账复用它)。
- **教训**:memtable v0 曾让 Reservation 在 put 返回时立即 drop → 限额永不生效。
  改为"一条累计 Reservation + resize(new_total)"模式。任何新记账点都应问一句:
  Reservation 的生命周期覆盖资源生命周期吗?

### 2.3 Arena
- bump 分配:chunk(默认 64KiB)→ chunk 内按 layout.align bump;单对象超大时
  独占 chunk(容量按需,提示容量 min(1<<24))。
- 安全边界:chunk 指针只在 Mutex 后访问,`unsafe impl Send/Sync`;
  对外提供 `alloc_slice<T: Copy>` 安全封装;直接 `NonNull` 是文档化的 unsafe 边界
  (reset/drop 后失效)。chunk 的 Reservation 随 Chunk drop 归还配额。
- TODO:chunk 空闲池、string-heap 与 vector-buffer 双 arena。

### 2.4 与执行的接线
- 每查询一个 `MemoryContext{tracker(child of session), arena}`;查询结束整体归还。
- `DataChunk::with_memory`:按 `approx_size()` 记账,chunk drop 自动归还 ——
  批的内存统计随对象生命周期,不漏不虚。
- 查询节点 `set_limit(query_limit_bytes)`;超限错误映射 SQLSTATE 53200。

## 3. 存储引擎(LSM)

### 3.1 内部键
- `user_key ++ [!seq 8B BE ++ kind 1B]`;比较器 = memcmp(user_key) + memcmp(trailer)。
- 不变式:**任何按内部键有序的介质(memtable/SST)中,同 user_key 新版本在前** →
  点查/合并迭代"第一条命中即最新"。
- 坑:原始字节 memcmp 不等于内部键序("ab" vs "abc"),必须用 `compare_internal`。

### 3.2 写路径(单写者)
```
write_batch: write_lock → seq 段(fetch_add n) → WAL 帧落盘 → memtable 插入
             └─ memtable 限额失败 → flush_locked → 重试一次
```
- 先 WAL 后 memtable:崩溃时宁可多恢复一条,不可丢已确认写。
- flush:swap 新 memtable → 旧表进 imm → 扫表写 tmp SST → rename 原子生效 →
  清单加 L0 → 归还记账 → WAL reset。顺序保证崩溃窗口不丢数据。

### 3.3 读路径
memtable → imm(新→旧)→ manifest 快照(L0 id 降序,L1)→ NotFound。
bloom + [min,max] range 短路;`covers` 用 user_key 闭区间。

### 3.4 SST 格式(v1)
```
[data block]…  块 = varint klen|key|varint vlen|value 串 + crc32(body)
[bloom block]  FNV-1a 双重哈希;varint bits + varint probes + 位图
[index block]  复用块格式:entry = (块内最大内部键, varint offset+varint len)
[footer 68B]   bloom_off/len, index_off/len, num_entries, min_seq, max_seq,
               version u32, magic "LTB1", crc32
```
- footer 携带 min/max seq:首尾 entry 只能界定 user_key 范围,不能界定 seq 范围
  (中间键可能带更大 seq),v0 在此踩过坑。
- 无 restart point:块内 seek 是线性扫(16KiB 块可接受);前缀压缩 TODO。
- v1 整文件驻留内存打开:简单、Windows 友好;块缓存 TODO(M4)。

### 3.5 压缩
- LeveledStrategy:L0 数 ≥ 阈值 → L0+L1 全量合并成单个 L1 文件。
  单 L1 不重叠 ⇒ 读路径无需"层内多文件 recency"推理,正确性优先。
- 合并 = 各输入 scan → 内部键排序 → 同 user_key 取首条(最新)→ 墓碑直接丢
  (无更低层级)。O(n log n),数据量上来换分级/流式。

### 3.6 恢复
- WAL 重放重建 memtable(`put_untracked`,恢复期不限额)。
- 扫 sst/ 目录按文件名 `{id:020}.{level}.sst` 重建清单;.tmp 残留清理。
- 两个来源的划分恰好覆盖"已 flush / 未 flush",无丢失窗口。

### 3.7 列存副本(HTAP)
- v1:`ColumnarReplica` trait + `InMemoryColumnarReplica`(BTreeMap 键→行,
  scan_column 按键序输出列向量,两次 scan 列内对齐)。
- 生产化:TP 写路径旁路 CDC → 副本增量;列存 segment 文件 + delta 行 +
  后台 merge;字典/RLE 编码;AP 算子直接消费列(exec 侧转换 ColumnVector)。

## 4. 向量化执行

- 物理:`Vector{ColumnData, Option<Validity>}`;数值列紧凑切片(内核零拷贝),
  Utf8 用 data+offsets(u32,≤4GiB/列),null 位图 1=valid,无 null 不带位图。
- 批:`DataChunk{columns, num_rows 不变式, Reservation}`;`select` 是 filter
  的核心动作(压缩所有列 + null 位图);`head` 供 limit。
- 表达式:`Expr::eval(chunk) -> Vector`,内核为切片上的紧密循环;
  算术 checked(溢出报错)、比较三值 null 传播、And/Or 暂按 false。
- 算子:pull 式 `next_batch(ctx)`;`Ok(None)` 终止。混合类型算术按侧提升
  (v0 曾对已提升侧重复提升 panic)。
- 计划器:语句级 `StatementPlan`(Query/Insert/InsertSelect/Update/Delete/
  CreateTable/AlterAddColumn/DropTable);查询流水线见 §4.8;输出 schema 静态推导
  (`Expr::return_type`)。计划器只读 catalog 与 `QueryDataSource` 接口
  (exec 定义、Engine 实现),保持 exec↔storage 解耦。
- 列解析:先精确后大小写不敏感(PG 未加引号标识符折叠的宽松近似);
  多表场景统一走 `Scope::resolve`(限定/非限定、扁平下标、歧义报错)。

## 4.8 多表 FROM 与 JOIN 计划(TPC-H v1)

以 TPC-H 查询形状为验收标准(`server/tests/tpch.rs`:Q1/Q3/Q6 简化形态)。

- **Scope**:`Vec<(别名, TableSchema)>` 有序表集合。列引用解析成**扁平下标**
  (表 k 占 `[offsets[k], offsets[k+1])`);非限定列在全部表中必须唯一命中,
  否则报歧义。别名 = 显式 alias 或表名,重名报错。
- **FROM 展开**:TableWithJoins 列表 → `Vec<RelationStep>`(第 0 个为驱动表,
  后续每个 JOIN/逗号连接是一个步骤)。`JOIN`/`INNER JOIN` 同义
  (sqlparser 0.64 是两个变体);USING/NATURAL 拒绝。
- **连接执行**:逐表 `SeqScan(+下推过滤)→ HashJoinExec`(probe = 已组合流水线,
  build = 新表)。等值条件 `a = b` 且两侧各只引用一张表(表达式亦可,如
  `l_extendedprice*(1-l_discount) = x`)→ 哈希键:probe 侧按扁平下标求值,
  build 侧列下标重基到表内(`rebase_columns`)。其余条件:
  - `match_cond`:匹配期逐对判定(LEFT JOIN 下未过条件的左行仍 NULL 补齐
    —— 若放 residual 会错误地把 NULL 补齐行过滤掉);
  - `residual`:连接后整批过滤(WHERE 路由下来的条件,作用于含 NULL 补齐行
    的完整结果 —— 这正是 WHERE 的语义位置)。
- **WHERE 合取项路由**(按引用表集合):
  1. 单表且其后无 LEFT 步骤 → 下推该表扫描;
  2. 单表且其后有 LEFT 步骤(保留侧/空产侧条件)→ 挂该 LEFT 步骤 residual
     (推扫描会改变 LEFT 语义:空产侧条件下推会把"不匹配"错误变成"匹配后过滤");
  3. 常量谓词 → 第一张表扫描;
  4. 跨表 `a = b`(INNER)→ 该步骤哈希键(在 `try_hash_eq` 中与 match_cond
     共用解析);
  5. 其余跨表条件 → 挂"引用的最大表位置"步骤 residual(该步之后两侧行都已
     可见,语义 = WHERE 在 FROM 之后求值)。
- **聚合走扁平 schema**:AggRewriter/AggHashExec 的输入是组合后的扁平列,
  GROUP BY / 聚合 / HAVING / ORDER BY 全部用扁平下标,无需感知连接边界。
- 已知限制:连接顺序按 FROM 书写(左深链),无代价优化;哈希键两侧类型不同时
  不匹配(标量相等按类型);build 侧整批物化。

## 4.9 DATE / DECIMAL / LIKE(v1 简化)

- **DATE** = 距 1970-01-01 的天数(i32,Hinnant civil_from_days/days_from_civil,
  有效范围约 ±500 万年)。`DATE 'YYYY-MM-DD'`(sqlparser TypedString)→ 字面量;
  `d ± INTERVAL 'n' DAY` → Date±Int64 内核;`d1 - d2` → 天数;
  `EXTRACT(YEAR/MONTH FROM d)` → year()/month() 标量函数;比较走 compare_values。
  Date 列与裸字符串字面量比较:字符串按目标类型解析(coerce_nonscalar_side)。
- **DECIMAL/NUMERIC(p,s) → Float64**:无精确小数类型,TPC-H 数值列够用;
  精确小数是后续独立类型工作。
- **LIKE / NOT LIKE**:`%`/`_` 带回溯匹配器;`ESCAPE`/`ILIKE` 暂不支持。

## 4.9.1 CASE / IN / 非相关子查询

- **CASE(搜索式)**:`Expr::Case { whens: Vec<(cond, result)>, else_expr }`,
  向量化求值 = 每分支整批求条件列与结果列,再逐行取首个命中分支
  (2m+1 次 eval + 行内选择);条件 null 按未命中(与 And/Or 简化一致);
  结果类型统一:任一分支 Float64 → Float64,Int64 字面量臂自动提升,
  其余不匹配报错(不静默)。简单式 `CASE <operand>` 拒绝。
  **聚合内 CASE**(`SUM(CASE WHEN ...)`):AggRewriter 对 Case 递归改写,
  条件/结果子树按聚合上下文解析 —— Q7/Q9/Q12/Q14 的关键形状。
- **IN 值列表**:解析期元素常量求值 → `Expr::InList { expr, list: Arc<Vec<SV>>,
  negated }`,执行为哈希集合成员判定(避免 OR 链展开);null 表达式 → false;
  成员判定标量精确相等(不跨类型)。
- **非相关子查询**:计划期执行一次(`eval_subquery_rows`:plan_query + 独立
  内存上下文整批收集;EXISTS 取到首个非空批即停)并物化:
  - 标量 `(SELECT ...)` → 必须恰 1 行 × 1 列 → Literal;
  - `IN (SELECT ...)` → 1 列 → InList 值集合;
  - `[NOT] EXISTS` → 布尔字面量。
  **相关引用直接报错**:子查询以空 scope 计划,引用外层别名/列会得到
  "unknown table alias / column not found"(相关子查询 decorrelation 见路线图)。
  局限:IN 子查询结果整体物化,大结果集的半连接(semi-join)见路线图。
- **顺带修复的存量 bug**:HAVING 引入投影中没有的聚合(如
  `HAVING SUM(x) > (SELECT ...)`)时,agg 输出 schema 在 HAVING 改写前计算 →
  谓词列下标越界。修复:HAVING 改写提前到 AggHashExec 构建**之前**,改写后
  重算 `project_input_schema`。

## 4.5 表数据布局与 Engine(M2)

- 行键 `table_id u64BE ++ row_id u64BE`:按表聚簇、扫描序=插入序。
  **row_id 派生自全局序列**(LsmStore::alloc_sequences),重启水位 =
  max(WAL 重放, 全部 SST max_seq)。v0 只看 WAL —— flush 后重启会回退,
  新插入复用 row_id 覆盖历史行;已修并加测试
  (`seq_watermark_survives_flush_only_restart`)。
- catalog 持久化:保留表 0,键 = `[0;8] ++ 表名`,值 = serde_json(TableSchema)。
  CREATE/DROP 同步写目录区;Engine::open 扫目录区重建 catalog。
  CREATE 先改内存 catalog 再落目录区,失败则回滚内存。
- INSERT:计划期求值 + 按列收敛(精确类型 / Int64→Float64 / NOT NULL 检查)→
  执行期 alloc_sequences 取 row_id 段 → 单个 write_batch(一条 WAL 记录)→
  列存副本逐行 upsert(失败仅告警,不回滚行存 —— 计划期已保证类型正确)。
- DELETE:整表扫描(键+行)→ 谓词在整批上向量化求值 → 命中键进写批打墓碑 →
  副本逐键 delete。
- DROP TABLE:catalog 移除 + 数据范围打墓碑 + 目录键墓碑,同一写批;副本来
  drop_table。同名重建分配新 table id,天然看不到旧数据。
- 多语句脚本:逐条执行,任一失败即停并返回错误(已执行的不回滚,无事务 —— M1)。

## 5. 服务端

- pgwire 0.42:`PgWireServerHandlers` 只覆写 `simple_query_handler`(返回
  `Arc<Self>`),启动用 `NoopStartupHandler` 免认证;accept loop 每连接
  `process_socket`。注意 0.42 没有 `PgWireServer` 结构,直接用 `process_socket`。
- sqlparser 0.64 AST 适配要点(升级时先对照):
  `Query.limit_clause: Option<LimitClause>`(枚举,两种语法形状);
  `SelectItem::ExprWithAliases` 与 `QualifiedWildcard(SelectItemQualifiedWildcardKind)`;
  `Insert.table: TableObject`、`Insert.columns: Vec<ObjectName>`;
  `Delete.from: FromTable` 枚举;`TableFactor::Table{..}`;`ObjectName`
  由 `ObjectNamePart`(Identifier/Function)组成,用 `as_ident()` 剥;
  `DataType::Int8(Option<u64>)` 等带 payload;`Values.rows: Vec<Parens<Vec<Expr>>>`。
- 行编码:`DataRowEncoder.encode_field(&Option<String>)`(NULL→-1),文本格式。
  命令标签:INSERT 用 prefix "INSERT 0" + with_rows(n) 拼出 "INSERT 0 n"。
- 错误映射:NotImplemented/NotSupported→0A000,MemoryLimitExceeded→53200,
  Catalog→42P01,Sql→42601,其余 XX000。
- v1 同步执行在 async 上下文:短查询可接受;扫描算子接入后换 spawn_blocking
  或把执行器做成 async(见路线图)。

## 5.5 统计信息与代价优化器

### 5.5.1 统计(`common::stats`,持久化于保留表 0)

- **布局**:键 = `[0;8] ++ [0x01] ++ 表名`(表名不可能以 0x01 开头,与 catalog
  键天然隔离;0x01 < 可打印字符 → 统计键聚在目录区最前)。值 = 手写编码
  (版本 + row_count + stale 标记 + 每列 {ndv, null_count, min/max codec 标量})。
  恢复与 catalog 同一次 scan;**统计键排在 catalog 键之前 → 两阶段恢复**
  (先收集,待 schema 全部挂载后再绑定 table_id)。
- **生命周期**:
  - row_count 精确增量:INSERT +n / DELETE -n / UPDATE 不变;
  - min/max 只扩不缩(删除不收窄 —— 永远是合法上界,估算安全);
  - NDV 写路径**不做**增量(增量 NDV 只会越数越错),只在 ANALYZE 时全量
    (或 >100 万行等步长抽样)计算;`ndv == 0` 表示"未统计过";
  - 事务提交 / DELETE 标记 `stale`;规划期 `needs_analyze()`(stale / 缺条目 /
    有列未统计且 row_count > 0)→ 自动 ANALYZE(同步、持久化)。
- **为什么规划期能写**:统计写是幂等的单键 put,与数据写互不冲突;
  自动 ANALYZE 的代价 = 一次全表扫描,对教学规模数据可接受(大表走抽样)。

### 5.5.2 选择性估算(`optimizer::Estimator`)

- 列 op 常量:等值 = 1/NDV × (1 - null_frac);范围 = min/max 线性插值
  (标量 → f64,Date 按天数),夹到 [0.001, 1];
- 列 = 列:1/max(NDV 两侧)(连接基数因子);表达式键 → 表基数/10 启发式;
- LIKE = 0.1;IS NULL = null_frac;NOT/OR 递归;形状不明 = 0.25。
- 无统计表:rows = 1000,ndv = rows/10 夹到 [1, 100]。

### 5.5.3 连接顺序枚举(`optimizer::optimize_join_order`)

- **条件池**:WHERE + INNER ON 的跨表(≥2 表)合取项;等值对
  (`a = b` 两侧各引一表)单独标记(键候选,选择性 1/max(NDV))。
- **DP**:节点集 ⊆ 10 张表时子集 DP。代价 = Σ 每步
  (累计读 + 新表读 + min(两侧)× BUILD_WEIGHT(2.0)+ 输出估行);
  每步物化侧 = 较小者(必要时交换 probe/build,输出经投影置换还原规范列序)。
  等价代价取 FROM 书写序(确定性);> 10 表贪心(每步选累计基数最小者)。
- **估计基数**:est(S) = Π est(表) × Π 生效条件选择性(引用 ⊆ S 的条件),
  下限 1 行。
- **LEFT JOIN 是屏障**:连续 INNER 段为重排组;LEFT 步骤固定在 FROM 位置
  (探测侧 = 屏障前全部,构建侧 = 该表,谓词不下推)。跨屏障的组通过
  VIRTUAL 虚拟节点(既有流水线,基数已知)接入 DP。
- **列序不变式**:每步连接后流水线列 = 已连接集合的规范(FROM)序,不一致即
  投影置换;`col_shift` 跟踪"位置 i ↔ 扁平 i + shift"(首表非表 0 的重排组
  shift ≠ 0),累计/新表两侧表达式按 shift 重基 —— 这是连接键/残余条件
  正确性的关键(踩过:重排后键引用错位 → Date = Float64 之类怪错)。
- **WHERE 路由不变式**(统一 WHERE + INNER ON):单表条件当且仅当其后
  (FROM 序 ≥ 自身)无 LEFT 步骤才下推扫描;否则延迟到该 LEFT 步骤连接后
  过滤。空产侧条件下推会把"匹配后过滤"变成"过滤后 NULL 补齐" —— 语义 bug,
  统一路由时修正。

### 5.5.4 EXPLAIN

`EXPLAIN <SELECT>` → `StatementPlan::Explain { lines }`(不执行查询)。
计划树自顶向下:Limit → Sort → Project → Aggregate → HashJoin → Scan,
每节点带估行数 / 下推过滤 / 连接键 / 物化侧标签。planner 以 `ExplNode` 树
累积、`flatten_expl` 摊平;LIMIT 在外层包装时整体缩进一级。

#### 5.5.5 读取路径:谓词下推 / AP 列存 / 并行

- **谓词下推**:`QueryDataSource::scan_table_filtered(table, pred)`(pred 为
  表内局部下标)。引擎实现 = LSM 扫描 + **并行解码过滤**:entries 分片 →
  各线程解码组批(512 行)→ 向量化求值 → 按片序拼接(保插入序)。
  planner 的 `table_scan` 把合取谓词直接传给 SeqScan(不再叠加 FilterExec)。
  收益:未命中行不进物化,JOIN build 侧/聚合输入/内存同步缩小。
- **AP 列存路径**:`QueryDataSource::scan_columns(table) -> Option<Vec<Vector>>`。
  引擎实现:列存副本逐列读出 → 转 exec Vector(含 null 位图转换)。
  - 门控(任一不满足即回退行存):事务内读(副本只镜像已提交状态,
    必须保读己之写)、LOB/数组/向量列、副本缺失;
  - **重启重建**:副本是内存镜像,Engine::open 时从 LSM 全量重建 ——
    空副本不可信,否则重启后新写入的行会"遮蔽"历史行(v1 踩过);
  - 列存副本 v1.1:每列带 null 位图(与行存 NULL 语义一致),Date 列支持;
  - 列间长度一致性校验(misalignment = corruption 错误)。
- **并行框架**(`exec::parallel`):`scope_map` / `scope_map_try` ——
  `std::thread::scope` 分片借用(无需 'static),连续分片保序拼接。
  应用:LSM 解码过滤(engine)、HashJoin 探测行组装(≥256 行才并行,
  避免小批线程开销)。`ExecuteContext.threads` / `config.engine.parallelism`
  (默认 CPU 核数,上限 8)向下传递。路线图:morsel-driven 流水线并行、
  并行哈希聚合(分区归并)、并行排序。

## 5.5.5 内存上限与物化记账

- **限额层级**:全局(默认 30 GiB)→ 会话 → 查询 → 执行器,任一层超限即
  `MemoryLimitExceeded`(SQLSTATE 53200),整条链路原子回滚记账。
- **记账缺口补齐**(此前只覆盖 DataChunk/arena/memtable):
  - SeqScan 物化行集:一次性 reserve(行集 × `row_approx_bytes`),扫完即释放;
  - HashJoin:build 哈希表逐行 reserve;输出缓冲(materialize 的 `out`)逐行
    reserve;`Reservation` 随执行器 drop 释放;
  - 引擎输出(`run_query` 收集的行):逐行 `Reservation::resize`。
- **教训**:行级 `Vec<Row>` 累积此前完全游离在配额树外,限额形同虚设;
  任何新的行/表物化都必须在创建点问一句"记账了吗"。
- **OR 公共键提取**(Q19 根因):`(...) OR (...) OR ...` 且每个析取分支都含
  同一跨表等值对(按两侧 SQL 文本匹配)→ 提取为哈希键,完整 OR 仍作 residual
  强制(`PoolEntry.key_from_or`)。此前这类查询无法识别连接键 → 笛卡尔积
  全物化(60k × 20k 行级),是基准内存爆掉的直接原因。

## 6. 测试策略

- 单元:每个格式(键/块/bloom/WAL/SST)codec 往返 + 损坏检测;
- 集成:LsmStore put/flush/compaction/恢复/并发写;PG 协议端到端(原生 TCP);
- 内存:配额树超限/回滚/并发、arena 对齐/增长/reset/配额联动;
- 优化器:选择性数学(NDV/插值/默认)、DP 顺序(避免无选择性的笛卡尔前缀)、
  虚拟节点;e2e:EXPLAIN 树形状、ANALYZE、统计增量/重启恢复、CBO 禁止
  无键连接(`server/tests/optimizer.rs`)。
- 新格式字段一律"写入端-读取端-测试"三方同改,footer 尺寸变更要 bump version。
