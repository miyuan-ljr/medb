//! JIT 编译执行教学示例(可运行):
//!   cargo run -p medb-exec --example jit_demo --release
//!
//! 用同一个表达式 `(2 + x) * 3 + x * 0.5 - 1` 展示三种执行方式:
//!   A. 解释执行:每行走一遍树(match + 递归 + 指针追逐)
//!   B. JIT 理想形态:泛型闭包组合 → rustc 单态化 → 整棵树内联成直线机器码
//!   C. 数据库的现实:表达式形状是运行期才知道的 → 只能解释或生成字节码
//!      (medb 的 expr_compile 就是 C;B 就是 HyPer 式 JIT 想达到的终点)
//!
//! 关键洞察:程序的输入分两类 ——
//!   * 每行都变的:列值 x(数据)
//!   * 从不变的:表达式结构(编译期已知)
//! JIT = 把"不变的部分"提前算掉/烘焙掉,生成只处理"可变部分"的专用代码。

use std::time::Instant;

// ---------- 0. 表达式树(共同输入) ----------

#[derive(Debug)]
enum Expr {
    Lit(f64),
    Var, // 输入变量 x
    Add(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
}

// ---------- A. 解释执行 ----------

fn eval(e: &Expr, x: f64) -> f64 {
    match e {
        Expr::Lit(c) => *c,
        Expr::Var => x,
        Expr::Add(a, b) => eval(a, x) + eval(b, x),
        Expr::Mul(a, b) => eval(a, x) * eval(b, x),
    }
}

// ---------- B. JIT 理想形态:泛型闭包组合 ----------

// 组合子:泛型参数让 rustc 为**每一次组合的具体形状**单独生成代码,
// 且 fa/fb 都是小闭包 → LLVM 全部内联 → 整棵树塌缩成直线机器码。
fn lit(c: f64) -> impl Fn(f64) -> f64 {
    move |_| c
}

fn var() -> impl Fn(f64) -> f64 {
    |x| x
}

fn add<FA: Fn(f64) -> f64, FB: Fn(f64) -> f64>(fa: FA, fb: FB) -> impl Fn(f64) -> f64 {
    move |x| fa(x) + fb(x)
}

fn mul<FA: Fn(f64) -> f64, FB: Fn(f64) -> f64>(fa: FA, fb: FB) -> impl Fn(f64) -> f64 {
    move |x| fa(x) * fb(x)
}

fn main() {
    let expr = Expr::Add(
        Box::new(Expr::Add(
            Box::new(Expr::Mul(
                Box::new(Expr::Add(Box::new(Expr::Lit(2.0)), Box::new(Expr::Var))),
                Box::new(Expr::Lit(3.0)),
            )),
            Box::new(Expr::Mul(Box::new(Expr::Var), Box::new(Expr::Lit(0.5)))),
        )),
        Box::new(Expr::Lit(1.0)),
    );
    println!("表达式: (2 + x) * 3 + x * 0.5 - 1\n");

    // —— B. JIT 理想形态:形状在编译期已知 → 单态化 → 直线代码 ——
    // 这一行就是"JIT 编译"的理想产物:树没了,只剩闭包组合,
    // rustc 单态化 + 内联后等价于手写 |x| (2.0 + x) * 3.0 + x * 0.5 - 1.0
    let compiled = add(
        add(mul(add(lit(2.0), var()), lit(3.0)), mul(var(), lit(0.5))),
        lit(1.0),
    );

    // 正确性
    for x in [0.0, 1.0, 7.25] {
        assert_eq!(eval(&expr, x), compiled(x), "x = {x}");
    }

    // —— 性能:1 亿次求值 ——
    let n = 100_000_000f64;
    let t = Instant::now();
    let mut acc_a = 0.0;
    for i in 0..n as u64 {
        acc_a += eval(&expr, i as f64 * 0.001);
    }
    let interp = t.elapsed();

    let t = Instant::now();
    let mut acc_b = 0.0;
    for i in 0..n as u64 {
        acc_b += compiled(i as f64 * 0.001);
    }
    let jit = t.elapsed();

    println!(
        "A 解释执行: {:>10.1?}   (逐行走树:match + 递归 + 指针追逐,每行 ~13 节点)",
        interp
    );
    println!(
        "B JIT 编译: {:>10.1?}   (直线机器码:类型/常量/拓扑全部烘焙,零树遍历)",
        jit
    );
    println!(
        "加速比:     {:>10.1}x",
        interp.as_secs_f64() / jit.as_secs_f64()
    );
    println!("校验: a = {acc_a:.0}, b = {acc_b:.0}");

    println!("\n但注意:B 之所以能做到,是因为表达式形状在【演示程序编译期】已知 ——");
    println!("数据库的表达式形状是【运行期】才知道的,所以:");
    println!("  · Box<dyn Fn> 闭包组合:消树遍历,但每节点仍有一次虚调用(收益小)");
    println!("  · 字节码 + 解释循环:medb expr_compile 的方案(扁平数组,缓存友好)");
    println!("  · Cranelift/LLVM:运行期生成真机器码,逼近 B(路线图)");
}
