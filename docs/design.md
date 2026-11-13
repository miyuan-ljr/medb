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

## 6. 测试策略

- 单元:每个格式(键/块/bloom/WAL/SST)codec 往返 + 损坏检测;
- 集成:LsmStore put/flush/compaction/恢复/并发写;PG 协议端到端(原生 TCP);
- 内存:配额树超限/回滚/并发、arena 对齐/增长/reset/配额联动。
- 新格式字段一律"写入端-读取端-测试"三方同改,footer 尺寸变更要 bump version。
