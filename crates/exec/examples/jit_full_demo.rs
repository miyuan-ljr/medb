//! 完整版 JIT:运行期把用户输入的表达式编译成**真机器码**。
//!
//!   cargo run -p medb-exec --example jit_full_demo --release
//!
//! 完整流水线(与 HyPer 同构):
//!   SQL 文本 → 解析 → AST → (常量折叠) → Cranelift IR → 原生机器码 → 函数指针
//!
//! 关键演示点:
//! 1. 表达式是**运行期从字符串解析**的(rustc 不可能预知形状);
//! 2. "编译器后端"只有十几行 —— 表达式树逐节点翻译成 Cranelift IR;
//! 3. 机器码性能 = jit_demo 里"编译期已知形状"的理想形态(28x 那条线);
//! 4. 编译有延迟(演示里实测),这正是 JIT 系统要对抗的开销。

use std::time::Instant;

use cranelift::codegen::settings::{self, Configurable};
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

// ---------- ① 解析:SQL 文本 → AST ----------
// 极简递归下降:支持 + - * /、数字、变量 x、括号。

#[derive(Debug, Clone)]
enum Expr {
    Lit(f64),
    Var,
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

struct Parser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s: s.as_bytes(),
            pos: 0,
        }
    }
    fn peek(&self) -> u8 {
        *self.s.get(self.pos).unwrap_or(&b' ')
    }
    fn skip_ws(&mut self) {
        while self.peek() == b' ' {
            self.pos += 1;
        }
    }
    fn expr(&mut self) -> Result<Expr, String> {
        let mut e = self.term()?;
        loop {
            self.skip_ws();
            match self.peek() {
                b'+' => {
                    self.pos += 1;
                    e = Expr::Add(Box::new(e), Box::new(self.term()?));
                }
                b'-' => {
                    self.pos += 1;
                    e = Expr::Sub(Box::new(e), Box::new(self.term()?));
                }
                _ => return Ok(e),
            }
        }
    }
    fn term(&mut self) -> Result<Expr, String> {
        let mut e = self.factor()?;
        loop {
            self.skip_ws();
            match self.peek() {
                b'*' => {
                    self.pos += 1;
                    e = Expr::Mul(Box::new(e), Box::new(self.factor()?));
                }
                b'/' => {
                    self.pos += 1;
                    e = Expr::Div(Box::new(e), Box::new(self.factor()?));
                }
                _ => return Ok(e),
            }
        }
    }
    fn factor(&mut self) -> Result<Expr, String> {
        self.skip_ws();
        match self.peek() {
            b'(' => {
                self.pos += 1;
                let e = self.expr()?;
                self.skip_ws();
                if self.peek() != b')' {
                    return Err("expected ')'".into());
                }
                self.pos += 1;
                Ok(e)
            }
            b'x' => {
                self.pos += 1;
                Ok(Expr::Var)
            }
            c if c.is_ascii_digit() => {
                let start = self.pos;
                while self.peek().is_ascii_digit() || self.peek() == b'.' {
                    self.pos += 1;
                }
                let text = std::str::from_utf8(&self.s[start..self.pos]).unwrap();
                text.parse::<f64>()
                    .map(Expr::Lit)
                    .map_err(|_| format!("bad number '{text}'"))
            }
            c => Err(format!("unexpected char '{}'", c as char)),
        }
    }
}

// ---------- ② 编译期优化:常量折叠(AST → 优化后 AST) ----------

fn fold(e: Expr) -> Expr {
    match e {
        Expr::Add(a, b) => match (fold(*a), fold(*b)) {
            (Expr::Lit(x), Expr::Lit(y)) => Expr::Lit(x + y),
            (na, nb) => Expr::Add(Box::new(na), Box::new(nb)),
        },
        Expr::Sub(a, b) => match (fold(*a), fold(*b)) {
            (Expr::Lit(x), Expr::Lit(y)) => Expr::Lit(x - y),
            (na, nb) => Expr::Sub(Box::new(na), Box::new(nb)),
        },
        Expr::Mul(a, b) => match (fold(*a), fold(*b)) {
            (Expr::Lit(x), Expr::Lit(y)) => Expr::Lit(x * y),
            (na, nb) => Expr::Mul(Box::new(na), Box::new(nb)),
        },
        Expr::Div(a, b) => match (fold(*a), fold(*b)) {
            (Expr::Lit(x), Expr::Lit(y)) if y != 0.0 => Expr::Lit(x / y),
            (na, nb) => Expr::Div(Box::new(na), Box::new(nb)),
        },
        other => other,
    }
}

// ---------- ③ 解释执行(基线) ----------

fn eval(e: &Expr, x: f64) -> f64 {
    match e {
        Expr::Lit(c) => *c,
        Expr::Var => x,
        Expr::Add(a, b) => eval(a, x) + eval(b, x),
        Expr::Sub(a, b) => eval(a, x) - eval(b, x),
        Expr::Mul(a, b) => eval(a, x) * eval(b, x),
        Expr::Div(a, b) => eval(a, x) / eval(b, x),
    }
}

// ---------- ④ Cranelift 后端:AST → IR → 机器码 ----------
// 这就是"JIT 编译器"的全部:表达式逐节点翻译成 IR 指令,
// 剩下的(指令选择 / 寄存器分配 / 机器码发射)Cranelift 全包。

fn codegen_expr(fb: &mut FunctionBuilder, e: &Expr, x: Value) -> Value {
    match e {
        Expr::Lit(c) => fb.ins().f64const(*c),
        Expr::Var => x,
        Expr::Add(a, b) => {
            let va = codegen_expr(fb, a, x);
            let vb = codegen_expr(fb, b, x);
            fb.ins().fadd(va, vb)
        }
        Expr::Sub(a, b) => {
            let va = codegen_expr(fb, a, x);
            let vb = codegen_expr(fb, b, x);
            fb.ins().fsub(va, vb)
        }
        Expr::Mul(a, b) => {
            let va = codegen_expr(fb, a, x);
            let vb = codegen_expr(fb, b, x);
            fb.ins().fmul(va, vb)
        }
        Expr::Div(a, b) => {
            let va = codegen_expr(fb, a, x);
            let vb = codegen_expr(fb, b, x);
            fb.ins().fdiv(va, vb)
        }
    }
}

/// 编译表达式 → 可调用的原生机器码函数(x: f64 -> f64)。
/// 返回 (函数指针, JITModule —— 机器码随 module 存活,不可先 drop)。
fn jit_compile(
    e: &Expr,
) -> Result<(unsafe extern "C" fn(f64) -> f64, JITModule), Box<dyn std::error::Error>> {
    let mut flag_builder = settings::builder();
    flag_builder.set("use_colocated_libcalls", "false").unwrap();
    flag_builder.set("is_pic", "false").unwrap();
    let isa_builder = cranelift_native::builder().expect("host CPU features");
    let isa = isa_builder.finish(settings::Flags::new(flag_builder))?;
    let frontend_config = isa.frontend_config();
    let mut module = JITModule::new(JITBuilder::with_isa(
        isa,
        cranelift_module::default_libcall_names(),
    ));

    // 函数签名:fn(x: f64) -> f64
    let mut ctx = module.make_context();
    ctx.func.signature.params.push(AbiParam::new(types::F64));
    ctx.func.signature.returns.push(AbiParam::new(types::F64));
    let func_id = module
        .declare_function("expr", Linkage::Export, &ctx.func.signature)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    // IR 构建:AST 逐节点翻译
    let mut fb_ctx = FunctionBuilderContext::new();
    let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);
    let entry = builder.create_block();
    builder.append_block_params_for_function_params(entry);
    builder.switch_to_block(entry);
    builder.seal_block(entry);

    let x = builder.block_params(entry)[0];
    let result = codegen_expr(&mut builder, e, x);
    builder.ins().return_(&[result]);
    builder.finalize(frontend_config);

    // 后端:指令选择 + 寄存器分配 + 机器码发射(Cranelift 全包)
    module
        .define_function(func_id, &mut ctx)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    module.clear_context(&mut ctx);
    module.finalize_definitions().unwrap();
    let code_ptr = module.get_finalized_function(func_id);
    let f: unsafe extern "C" fn(f64) -> f64 = unsafe { std::mem::transmute(code_ptr) };
    Ok((f, module))
}

// ---------- 演示 ----------

fn main() {
    // ① 用户输入的 SQL 表达式(运行期文本,rustc 不可能预知)
    let sql = "(2 + x) * 3 + x * 0.5 - 1";
    println!("表达式(运行期文本): {sql}\n");

    // ② 解析 → AST
    let t = Instant::now();
    eprintln!("[stage] parsing...");
    let mut ast = Parser::new(sql).expr().unwrap();
    let parse_time = t.elapsed();
    println!("① 解析:     {:?}  ({:?})", ast, parse_time);

    // ③ 编译期优化:常量折叠((2+x)*3 里没有可折叠常量,但机制在这)
    ast = fold(ast);

    // ④ 解释执行基线
    eprintln!("[stage] interpreting 100M rows...");
    let t = Instant::now();
    let mut acc_i = 0.0;
    let n = 100_000_000f64;
    for i in 0..n as u64 {
        acc_i += eval(&ast, i as f64 * 0.001);
    }
    let interp = t.elapsed();

    // ⑤ JIT:AST → 机器码(含编译延迟 —— JIT 系统要对抗的成本)
    eprintln!("[stage] cranelift codegen...");
    let t = Instant::now();
    let (code_fn, _module) = jit_compile(&ast).unwrap();
    let codegen_time = t.elapsed();
    println!(
        "② Cranelift 编译: {:?}  (AST → IR → 机器码,含编译延迟)",
        codegen_time
    );
    println!("   产物: fn(x: f64) -> f64 的原生机器码 + 函数指针\n");

    // 正确性
    for x in [0.0, 1.0, 7.25] {
        assert_eq!(eval(&ast, x), unsafe { code_fn(x) }, "x = {x}");
    }

    // ⑥ 机器码执行(与 jit_demo 里 28x 的"理想形态"同档)
    eprintln!("[stage] machine code 100M rows...");
    let t = Instant::now();
    let mut acc_m = 0.0;
    for i in 0..n as u64 {
        acc_m += unsafe { code_fn(i as f64 * 0.001) };
    }
    let machine = t.elapsed();

    println!("A 解释执行: {:>10.1?}   acc = {acc_i:.0}", interp);
    println!(
        "B JIT 机器码: {:>10.1?}   acc = {acc_m:.0}   (加速 {:.1}x)",
        machine,
        interp.as_secs_f64() / machine.as_secs_f64()
    );
    println!("\n要点:");
    println!("  · 形状是运行期从字符串来的 —— rustc 单态化帮不上,必须运行期生成机器码");
    println!("  · 编译延迟 {codegen_time:?} 就是 JIT 的成本;TP 点查要靠计划缓存摊薄");
    println!("  · HyPer 式全流水线 JIT = 把 scan/filter/agg 也编进同一个循环里");
}
