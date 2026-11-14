//! 表达式编译:Expr 树 → 扁平字节码(SSA 寄存器程序)+ 栈式虚拟机。
//!
//! 这是"编译执行"思想的**教学级完整实现**——JIT 的本质不是"跑得像原生代码"
//! 这么玄,而是把**编译器的三板斧**应用到查询计划上:
//!
//! 1. **常量折叠**(constant folding):纯常量子树在编译期算成单个字面量;
//! 2. **代数化简**(algebraic simplification):`x + 0 → x`、`x * 1 → x`……
//!    注意 SQL 三值逻辑下 **x*0 ↛ 0**(x 为 NULL 时 x*0 是 NULL),这类
//!    "数学正确但 SQL 错误"的折叠必须禁止 —— 注释里有完整说明;
//! 3. **公共子表达式消除**(CSE):相同子树只算一次,寄存器复用;
//! 4. **扁平化 + 单次使用移动**:树遍历的递归 + match 分发变成顺序指令
//!    循环;只被消费一次的寄存器按"移动"取用(省整列拷贝)。
//!
//! 执行模型:每条指令操作**整列向量**(寄存器 = Vector),与向量化模型
//! 完全兼容;虚拟机的每批开销 = 一次顺序循环,无递归无分发树。
//!
//! 与 LLVM 式 JIT 的关系:这里生成的是**字节码 + 解释循环**(③ 层表达式
//! 编译);① 层(全流水线机器码,LLVM/Cranelift)在此结构上加一个
//! "Op → 原生指令"的后端即可,寄存器分配思想完全一致。

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use common::error::{Error, Result};
use common::types::{DataType, ScalarValue};

use crate::chunk::DataChunk;
use crate::expr::{BinaryOp, Expr, broadcast, eval_binary};
use crate::vector::Vector;

// ---------- 字节码 ----------

/// 一条指令:dst 为目标寄存器,操作数一律为寄存器号(SSA 风格)。
#[derive(Debug, Clone)]
pub enum Op {
    /// r[dst] ← 输入批的第 col 列
    LoadCol { dst: usize, col: usize },
    /// r[dst] ← 字面量(广播到批行数)
    LoadLit { dst: usize, lit: ScalarValue },
    /// r[dst] ← r[lhs] op r[rhs]
    Binary {
        dst: usize,
        op: BinaryOp,
        lhs: usize,
        rhs: usize,
    },
    /// r[dst] ← 标量函数(name)(r[srcs])
    InvokeFunc {
        dst: usize,
        name: String,
        srcs: Vec<usize>,
    },
    /// r[dst] ← r[src] IS [NOT] NULL(布尔列)
    IsNull {
        dst: usize,
        src: usize,
        negated: bool,
    },
    /// r[dst] ← r[src] [NOT] IN 常量集合
    InList {
        dst: usize,
        src: usize,
        list: Arc<Vec<ScalarValue>>,
        negated: bool,
    },
    /// CASE:取首个 cond 为真的 val,否则 else(缺省 NULL)
    CaseSelect {
        dst: usize,
        pairs: Vec<(usize, usize)>,
        else_reg: Option<usize>,
    },
    /// r[dst] ← r[src](CSE 复用 / 重命名)
    Copy { dst: usize, src: usize },
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Op::LoadCol { dst, col } => write!(f, "r{dst} = col[{col}]"),
            Op::LoadLit { dst, lit } => write!(f, "r{dst} = lit {lit}"),
            Op::Binary { dst, op, lhs, rhs } => {
                write!(f, "r{dst} = r{lhs} {} r{rhs}", op.symbol())
            }
            Op::InvokeFunc { dst, name, srcs } => {
                let args = srcs
                    .iter()
                    .map(|r| format!("r{r}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "r{dst} = {name}({args})")
            }
            Op::IsNull { dst, src, negated } => {
                if *negated {
                    write!(f, "r{dst} = r{src} IS NOT NULL")
                } else {
                    write!(f, "r{dst} = r{src} IS NULL")
                }
            }
            Op::InList {
                dst, src, negated, ..
            } => {
                if *negated {
                    write!(f, "r{dst} = r{src} NOT IN ?set")
                } else {
                    write!(f, "r{dst} = r{src} IN ?set")
                }
            }
            Op::CaseSelect { dst, .. } => write!(f, "r{dst} = CASE ?regs END"),
            Op::Copy { dst, src } => write!(f, "r{dst} = r{src}"),
        }
    }
}

/// 编译产物:扁平指令序列 + 寄存器使用计数 + 结果寄存器。
#[derive(Debug, Clone)]
pub struct CompiledExpr {
    ops: Vec<Op>,
    /// 每个寄存器被后续指令消费的次数(单次使用 → VM 可移动,免整列拷贝)
    uses: Vec<u32>,
    nregs: usize,
    result: usize,
}

impl CompiledExpr {
    /// 字面量快捷构造(零指令)。
    pub fn literal(lit: ScalarValue) -> Self {
        Self {
            ops: vec![],
            uses: vec![0],
            nregs: 1,
            result: 0,
        }
        .with_result_lit(lit)
    }

    fn with_result_lit(mut self, lit: ScalarValue) -> Self {
        self.ops.push(Op::LoadLit { dst: 0, lit });
        self.uses = vec![0];
        self.result = 0;
        self
    }

    /// 字节码指令数(测试/EXPLAIN 用)。
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl fmt::Display for CompiledExpr {
    /// 程序清单:JIT 优化的直接证据 —— 常量折叠后的字面量、消失的冗余算子、
    /// CSE 后的寄存器复用,全部肉眼可见。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "program ({} ops, result = r{}):",
            self.ops.len(),
            self.result
        )?;
        for (i, op) in self.ops.iter().enumerate() {
            writeln!(f, "  [{i:03}] {op}")?;
        }
        Ok(())
    }
}

// ---------- 编译器 ----------

struct Compiler {
    ops: Vec<Op>,
    uses: Vec<u32>,
    nregs: usize,
    result: usize,
    /// CSE 表:子树文本 → 已存结果寄存器(Display 是确定性的,文本即结构键)
    cse: HashMap<String, usize>,
}

impl Compiler {
    fn new() -> Self {
        Self {
            ops: Vec::new(),
            uses: Vec::new(),
            nregs: 0,
            result: usize::MAX,
            cse: HashMap::new(),
        }
    }

    fn alloc_reg(&mut self) -> usize {
        let r = self.nregs;
        self.nregs += 1;
        self.uses.push(0);
        r
    }

    fn emit(&mut self, op: Op) -> usize {
        let dst = match &op {
            Op::LoadCol { dst, .. }
            | Op::LoadLit { dst, .. }
            | Op::Binary { dst, .. }
            | Op::InvokeFunc { dst, .. }
            | Op::IsNull { dst, .. }
            | Op::InList { dst, .. }
            | Op::CaseSelect { dst, .. }
            | Op::Copy { dst, .. } => *dst,
        };
        self.ops.push(op);
        dst
    }

    /// 编译子表达式 → 结果寄存器(操作数被消费一次,uses 计数 +1)。
    fn compile_sub(&mut self, e: &Expr) -> usize {
        let reg = self.lower(e);
        self.uses[reg] += 1;
        reg
    }

    /// 主入口:应用编译期优化后下沉为指令。
    fn lower(&mut self, e: &Expr) -> usize {
        // --- CSE:相同子树复用已有寄存器(跨整棵树,不限兄弟) ---
        let text = format!("{e}");
        if let Some(&reg) = self.cse.get(&text) {
            return reg;
        }
        let reg = self.lower_uncached(e);
        self.cse.insert(text, reg);
        reg
    }

    fn lower_uncached(&mut self, e: &Expr) -> usize {
        match e {
            // --- 常量折叠:纯常量子树在编译期算成单个字面量 ---
            Expr::Literal(v) => {
                let dst = self.alloc_reg();
                self.emit(Op::LoadLit {
                    dst,
                    lit: v.clone(),
                });
                dst
            }
            _ if is_pure_const(e) => {
                let dst = self.alloc_reg();
                let lit = fold_const(e);
                self.emit(Op::LoadLit { dst, lit });
                dst
            }
            Expr::Column(i) => {
                let dst = self.alloc_reg();
                self.emit(Op::LoadCol { dst, col: *i });
                dst
            }
            Expr::Binary { op, left, right } => {
                // 先折叠纯常量子树,再代数化简(编译器多轮迭代:
                // 折叠让 `x * (1-0)` 露出字面量 1,化简才能触发)
                let left = maybe_fold_const(left);
                let right = maybe_fold_const(right);
                // --- 代数化简(SQL 三值逻辑下安全的恒等式)---
                if let Some(operand) = algebraic_identity(*op, &left, &right) {
                    return self.lower(&operand);
                }
                let lhs = self.compile_sub(&left);
                let rhs = self.compile_sub(&right);
                let dst = self.alloc_reg();
                self.emit(Op::Binary {
                    dst,
                    op: *op,
                    lhs,
                    rhs,
                });
                dst
            }
            Expr::Function { name, args } => {
                let srcs = args.iter().map(|a| self.compile_sub(a)).collect::<Vec<_>>();
                let dst = self.alloc_reg();
                self.emit(Op::InvokeFunc {
                    dst,
                    name: name.clone(),
                    srcs,
                });
                dst
            }
            Expr::IsNull { expr, negated } => {
                let src = self.compile_sub(expr);
                let dst = self.alloc_reg();
                self.emit(Op::IsNull {
                    dst,
                    src,
                    negated: *negated,
                });
                dst
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let src = self.compile_sub(expr);
                let dst = self.alloc_reg();
                self.emit(Op::InList {
                    dst,
                    src,
                    list: Arc::clone(list),
                    negated: *negated,
                });
                dst
            }
            Expr::Case { whens, else_expr } => {
                let mut pairs = Vec::with_capacity(whens.len());
                for (c, r) in whens {
                    let creg = self.compile_sub(c);
                    let rreg = self.compile_sub(r);
                    pairs.push((creg, rreg));
                }
                let else_reg = else_expr.as_ref().map(|e| self.compile_sub(e));
                let dst = self.alloc_reg();
                self.emit(Op::CaseSelect {
                    dst,
                    pairs,
                    else_reg,
                });
                dst
            }
        }
    }

    fn finish(self) -> CompiledExpr {
        CompiledExpr {
            ops: self.ops,
            uses: self.uses,
            nregs: self.nregs,
            result: self.result,
        }
    }
}

/// 纯常量子树:仅字面量 + 可折叠运算(见 fold_const)。
fn is_pure_const(e: &Expr) -> bool {
    match e {
        Expr::Literal(_) => true,
        Expr::Binary { op, left, right } => {
            matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) && is_pure_const(left)
                && is_pure_const(right)
        }
        Expr::Function { name, args } => {
            // 无副作用、无列依赖的标量函数可折叠
            matches!(
                name.as_str(),
                "upper"
                    | "lower"
                    | "abs"
                    | "round"
                    | "floor"
                    | "ceil"
                    | "ceiling"
                    | "sqrt"
                    | "length"
                    | "trim"
                    | "reverse"
            ) && args.iter().all(is_pure_const)
        }
        _ => false,
    }
}

/// 常量折叠:利用现有向量化内核在 1 行 dummy 批上求值(同一套语义,零重复实现
/// —— 编译器复用解释器的内核,正是真实 JIT "runtime 联动编译" 的做法)。
fn fold_const(e: &Expr) -> ScalarValue {
    let lit_row = vec![ScalarValue::Int64(0)];
    let chunk =
        DataChunk::from_rows(&[common::types::DataType::Int64], &[lit_row]).expect("1-row chunk");
    e.eval(&chunk)
        .map(|v| v.scalar_at(0))
        .unwrap_or(ScalarValue::Null)
}

/// 纯常量子树 → 折叠后的字面量表达式(否则原样返回克隆)。
fn maybe_fold_const(e: &Expr) -> Expr {
    if is_pure_const(e) {
        Expr::Literal(fold_const(e))
    } else {
        e.clone()
    }
}

/// 代数化简:返回"可替换的等价子表达式"。
/// 仅收录 **SQL 三值逻辑下安全** 的恒等式 —— NULL 参与运算结果恒为 NULL:
/// - `x + 0 → x`、`0 + x → x`、`x - 0 → x` NULL + 0 = NULL = x ✓
/// - `x * 1 → x`、`1 * x → x`、`x / 1 → x`             ✓
/// - `x * 0 → 0` ✗ 禁止!x 为 NULL 时 x*0 是 NULL 不是 0
/// - `x - x → 0` ✗ 禁止!同理
fn algebraic_identity(op: BinaryOp, left: &Expr, right: &Expr) -> Option<Expr> {
    let lit_zero_or_one = |e: &Expr, want: u64| -> bool {
        match e {
            Expr::Literal(ScalarValue::Int64(v)) => (*v as u64) == want,
            Expr::Literal(ScalarValue::Float64(v)) => *v == want as f64,
            _ => false,
        }
    };
    let is_zero = |e: &Expr| lit_zero_or_one(e, 0);
    let is_one = |e: &Expr| lit_zero_or_one(e, 1);
    match op {
        BinaryOp::Add => {
            if is_zero(right) {
                Some((*left).clone())
            } else if is_zero(left) {
                Some((*right).clone())
            } else {
                None
            }
        }
        BinaryOp::Sub => {
            if is_zero(right) {
                Some((*left).clone())
            } else {
                None
            }
        }
        BinaryOp::Mul => {
            if is_one(right) {
                Some((*left).clone())
            } else if is_one(left) {
                Some((*right).clone())
            } else {
                None
            }
        }
        BinaryOp::Div => {
            if is_one(right) {
                Some((*left).clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------- 编译入口 ----------

/// Expr 树 → CompiledExpr(执行前调用一次;每个 chunk 复用同一程序)。
pub fn compile(e: &Expr) -> CompiledExpr {
    let mut c = Compiler::new();
    let result = c.lower(e);
    c.uses[result] += 1;
    c.result = result;
    c.finish()
}

// ---------- 虚拟机 ----------

impl CompiledExpr {
    /// 对整批求值(替代 Expr::eval 的扁平化路径)。
    pub fn eval(&self, chunk: &DataChunk) -> Result<Vector> {
        let n = chunk.num_rows();
        let mut regs: Vec<Option<Vector>> = vec![None; self.nregs];
        let mut uses = self.uses.clone();
        // 单次使用 → 移动(免整列拷贝);多次 → 克隆
        macro_rules! operand {
            ($reg:expr) => {{
                let reg: usize = *$reg;
                let taken = if uses[reg] <= 1 {
                    regs[reg]
                        .take()
                        .ok_or_else(|| Error::internal("compiled reg taken twice"))
                } else {
                    uses[reg] -= 1;
                    regs[reg]
                        .as_ref()
                        .ok_or_else(|| Error::internal("compiled reg read before write"))
                        .cloned()
                };
                taken?
            }};
        }
        for op in &self.ops {
            match op {
                Op::LoadCol { dst, col } => {
                    regs[*dst] = Some(chunk.column(*col).clone());
                }
                Op::LoadLit { dst, lit } => {
                    regs[*dst] = Some(broadcast(lit, n)?);
                }
                Op::Binary { dst, op, lhs, rhs } => {
                    let l = operand!(lhs);
                    let r = operand!(rhs);
                    regs[*dst] = Some(eval_binary(*op, &l, &r)?);
                }
                Op::InvokeFunc { dst, name, srcs } => {
                    let mut cols = Vec::with_capacity(srcs.len());
                    for r in srcs {
                        cols.push(operand!(r));
                    }
                    regs[*dst] = Some(crate::functions::eval_function(name, &cols)?);
                }
                Op::IsNull { dst, src, negated } => {
                    let v = operand!(src);
                    let out: Vec<bool> = (0..v.len()).map(|i| v.null_at(i) != *negated).collect();
                    regs[*dst] = Some(Vector::bools(out));
                }
                Op::InList {
                    dst,
                    src,
                    list,
                    negated,
                } => {
                    let v = operand!(src);
                    let set: std::collections::HashSet<&ScalarValue> =
                        list.iter().filter(|x| !x.is_null()).collect();
                    let out: Vec<bool> = (0..v.len())
                        .map(|i| {
                            if v.null_at(i) {
                                return false;
                            }
                            let hit = set.contains(&v.scalar_at(i));
                            if *negated { !hit } else { hit }
                        })
                        .collect();
                    regs[*dst] = Some(Vector::bools(out));
                }
                Op::CaseSelect {
                    dst,
                    pairs,
                    else_reg,
                } => {
                    // 条件/结果寄存器只读 → 克隆引用数据(? 不能进闭包,显式循环)
                    let mut conds = Vec::with_capacity(pairs.len());
                    let mut results = Vec::with_capacity(pairs.len());
                    for (c, r) in pairs {
                        let cv = regs[*c]
                            .as_ref()
                            .ok_or_else(|| Error::internal("case cond reg"))?;
                        let cb = cv.bool_values().to_vec();
                        let cn = (0..cv.len()).map(|i| cv.null_at(i)).collect::<Vec<_>>();
                        conds.push((cb, cn));
                        let rv = regs[*r]
                            .as_ref()
                            .ok_or_else(|| Error::internal("case val reg"))?;
                        results.push(rv.clone());
                    }
                    let else_vec = match else_reg {
                        Some(r) => regs[*r].clone(),
                        None => None,
                    };
                    // 类型统一(与 Expr::Case 求值同规则)
                    let mut has_f64 = false;
                    let mut first: Option<common::types::DataType> = None;
                    for rv in results.iter().chain(else_vec.iter()) {
                        let t: Option<DataType> = Some(rv.data_type());
                        if let Some(t) = t {
                            if t == common::types::DataType::Float64 {
                                has_f64 = true;
                            }
                            if first.is_none() {
                                first = Some(t);
                            }
                        }
                    }
                    let ty = if has_f64 {
                        common::types::DataType::Float64
                    } else {
                        first.unwrap_or(common::types::DataType::Int64)
                    };
                    let mut out = Vec::with_capacity(n);
                    for i in 0..n {
                        let mut picked = None;
                        for (ci, (cb, cn)) in conds.iter().enumerate() {
                            if !cn[i] && cb[i] {
                                picked = Some(results[ci].scalar_at(i));
                                break;
                            }
                        }
                        let raw = picked.unwrap_or_else(|| match &else_vec {
                            Some(ev) => ev.scalar_at(i),
                            None => ScalarValue::Null,
                        });
                        out.push(coerce_case(raw, &ty)?);
                    }
                    regs[*dst] = Some(Vector::from_scalars(ty, out)?);
                }
                Op::Copy { dst, src } => {
                    regs[*dst] = Some(operand!(src));
                }
            }
        }
        let last = self.result;
        regs[last]
            .take()
            .ok_or_else(|| Error::internal("compiled result reg empty"))
    }
}

/// 与 Expr::Case 求值一致的分支值收敛(仅 Int64→Float64 提升)。
fn coerce_case(v: ScalarValue, ty: &common::types::DataType) -> Result<ScalarValue> {
    use common::types::DataType;
    if v.is_null() {
        return Ok(ScalarValue::Null);
    }
    match ty {
        DataType::Float64 => match v {
            ScalarValue::Int64(i) => Ok(ScalarValue::Float64(i as f64)),
            ScalarValue::Float64(_) => Ok(v),
            other => Err(Error::sql(format!(
                "CASE branch type mismatch: expected Float64, got {other:?}"
            ))),
        },
        t if v.data_type().is_some_and(|vt| vt == *t) => Ok(v),
        _ => Err(Error::sql(format!(
            "CASE branch type mismatch: expected {ty:?}, got {v:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::schema::{ColumnDesc, Row};
    use common::types::DataType;
    use std::sync::Arc as StdArc;

    fn cols() -> Vec<ColumnDesc> {
        vec![
            ColumnDesc::new("price", DataType::Float64, true),
            ColumnDesc::new("disc", DataType::Float64, true),
            ColumnDesc::new("qty", DataType::Int64, true),
            ColumnDesc::new("name", DataType::Utf8, true),
        ]
    }

    fn chunk() -> DataChunk {
        let rows: Vec<Row> = vec![
            vec![
                ScalarValue::Float64(100.0),
                ScalarValue::Float64(0.25),
                ScalarValue::Int64(10),
                ScalarValue::Utf8("alpha".into()),
            ],
            vec![
                ScalarValue::Float64(200.0),
                ScalarValue::Float64(0.5),
                ScalarValue::Int64(20),
                ScalarValue::Utf8("beta".into()),
            ],
            vec![
                ScalarValue::Null,
                ScalarValue::Float64(0.1),
                ScalarValue::Int64(30),
                ScalarValue::Utf8("gamma".into()),
            ],
        ];
        let dtypes: Vec<DataType> = cols().iter().map(|c| c.ty.clone()).collect();
        DataChunk::from_rows(&dtypes, &rows).unwrap()
    }

    fn lit_f(x: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(x))
    }
    fn lit_i(x: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(x))
    }
    fn col(i: usize) -> Expr {
        Expr::Column(i)
    }
    fn bin(op: BinaryOp, l: Expr, r: Expr) -> Expr {
        Expr::binary(op, l, r)
    }

    /// JIT 优化①:常量折叠 —— (1+2)*3 编译后只剩一个字面量。
    #[test]
    fn jit_folds_constants() {
        let e = bin(
            BinaryOp::Mul,
            bin(BinaryOp::Add, lit_i(1), lit_i(2)),
            lit_i(3),
        );
        let c = compile(&e);
        assert_eq!(c.len(), 1, "整棵树应折叠为一条指令:\n{c}");
        assert_eq!(
            c.eval(&chunk()).unwrap().scalar_at(0),
            ScalarValue::Int64(9)
        );
    }

    /// JIT 优化②:代数化简 —— x + 0 → x(x*0 ↛ 0,SQL NULL 语义)。
    #[test]
    fn jit_applies_algebraic_simplification() {
        let e = bin(BinaryOp::Add, col(0), lit_f(0.0));
        let c = compile(&e);
        assert_eq!(c.len(), 1, "x+0 应消失:\n{c}"); // 只剩 LoadCol
        // NULL 语义保持:x(NULL) + 0 = NULL = x
        let out = c.eval(&chunk()).unwrap();
        assert_eq!(out.scalar_at(2), ScalarValue::Null);
        assert_eq!(out.scalar_at(0), ScalarValue::Float64(100.0));

        // 危险折叠被禁止:x * 0 不得折叠为 0(NULL 传播)
        let e2 = bin(BinaryOp::Mul, col(0), lit_f(0.0));
        let c2 = compile(&e2);
        assert!(c2.len() >= 2, "x*0 必须保留乘法(NULL 传播):\n{c2}");
        let out2 = c2.eval(&chunk()).unwrap();
        assert_eq!(out2.scalar_at(2), ScalarValue::Null);
    }

    /// JIT 优化③:CSE —— (a+b)*(a+b) 只算一次加法。
    #[test]
    fn jit_eliminates_common_subexpressions() {
        let ab = bin(BinaryOp::Add, col(0), col(1));
        let e = bin(BinaryOp::Mul, ab.clone(), ab);
        let c = compile(&e);
        let adds = c
            .ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    Op::Binary {
                        op: BinaryOp::Add,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(adds, 1, "加法应只算一次:\n{c}");
        // 数值正确性
        let out = c.eval(&chunk()).unwrap();
        assert_eq!(
            out.scalar_at(0),
            ScalarValue::Float64((100.0 + 0.25) * (100.0 + 0.25))
        );
    }

    /// 扁平化 + 单次使用移动:复杂表达式与解释执行**结果逐位一致**。
    #[test]
    fn compiled_equals_interpreted_on_complex_expr() {
        // Q7 形状:price * (1 - disc) * qty + length(name)
        let e = bin(
            BinaryOp::Add,
            bin(
                BinaryOp::Mul,
                col(0),
                bin(BinaryOp::Sub, lit_f(1.0), col(1)),
            ),
            Expr::Function {
                name: "length".into(),
                args: vec![col(3)],
            },
        );
        let compiled = compile(&e);
        let direct = e.eval(&chunk()).unwrap();
        let via_jit = compiled.eval(&chunk()).unwrap();
        for i in 0..3 {
            assert_eq!(
                direct.scalar_at(i),
                via_jit.scalar_at(i),
                "row {i} 编译执行与解释执行不一致"
            );
        }
    }

    /// 教学 demo(--nocapture 可见):Q7 形状表达式的编译前后对比。
    #[test]
    fn jit_program_listing_demo() {
        let e = bin(
            BinaryOp::Add,
            bin(
                BinaryOp::Mul,
                col(0),
                bin(BinaryOp::Sub, lit_f(1.0), lit_f(0.0)), // (1 - 0) 应被代数化简掉
            ),
            lit_f(0.0), // + 0 应被代数化简掉
        );
        let c = compile(&e);
        println!("== 编译前:树遍历(每 chunk 递归 + match 分发)\n{e}");
        println!("== 编译后:扁平程序(优化已应用)\n{c}");
        // (1-0)*x + 0 → x(两条冗余运算全部消失)
        assert_eq!(c.len(), 1, "\n{c}");
        let out = c.eval(&chunk()).unwrap();
        assert_eq!(out.scalar_at(0), ScalarValue::Float64(100.0));
    }
}
