//! medb 启动入口:加载配置 → 初始化内存机制 → 启动 PG wire 服务。

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use common::config::Config;
use tracing::{error, info};

/// 轻量级 HTAP 数据库(行存 LSM 主库 + 列存副本,向量化执行)。
#[derive(Parser)]
#[command(name = "medb", version, about)]
struct Cli {
    /// 配置文件路径(TOML)
    #[arg(long, short)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// 启动 PG wire 服务
    Serve,
}

fn init_tracing(config: &Config) {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| config.log.level.clone());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let (config, loaded) = match &cli.config {
        Some(path) => match Config::load(path) {
            Ok(c) => (c, true),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
        None => match Config::load_or_default(None) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(2);
            }
        },
    };
    if loaded {
        info!("config loaded");
    } else {
        info!("using default config (no config file given)");
    }
    init_tracing(&config);

    info!(
        allocator = memory::global::allocator_name(),
        global_limit_bytes = config.memory.global_limit_bytes,
        query_limit_bytes = config.memory.query_limit_bytes,
        batch_size = config.engine.batch_size,
        "memory mechanism ready"
    );

    match cli.command {
        Some(Command::Serve) | None => {
            match server::serve(config).await {
                Ok((bound, _state)) => {
                    info!(
                        %bound,
                        "medb is ready; connect with: psql -h 127.0.0.1 -p {} -U postgres",
                        bound.port()
                    );
                    // 常驻:等待 Ctrl+C
                    tokio::signal::ctrl_c().await.ok();
                    info!("shutting down");
                }
                Err(e) => {
                    error!(error = %e, "failed to start server");
                    std::process::exit(1);
                }
            }
        }
    }
}
