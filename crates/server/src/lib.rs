//! medb 服务端:PG wire 协议接入(客户端用现成的 psql / PG 驱动)。
//!
//! v1 只挂 simple query handler(启动用 pgwire 的 NoopStartupHandler 免认证);
//! TLS / extended query / COPY 见路线图。

pub mod engine;
pub mod handlers;
pub mod session;

use std::sync::Arc;

use common::config::Config;
use common::error::{Error, Result};
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use engine::Engine;
use handlers::{Handlers, ServerState};

/// 打开引擎并开始服务。返回监听地址与引擎句柄(测试/运维用)。
pub async fn serve(config: Config) -> Result<(std::net::SocketAddr, Arc<Engine>)> {
    // 引擎打开是阻塞 IO(目录扫描/WAL 重放),进程启动期一次,可接受
    let engine = Engine::open(config)?;
    let addr = engine.config().server.listen_addr.clone();
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| Error::internal(format!("bind {addr}: {e}")))?;
    let bound = listener.local_addr()?;
    let state = ServerState { engine };
    let returned_engine = state.engine.clone();
    tokio::spawn(async move {
        if let Err(e) = run_listener(listener, state).await {
            warn!(error = %e, "listener terminated");
        }
    });
    Ok((bound, returned_engine))
}

/// accept loop:每连接一个任务。
pub async fn run_listener(listener: TcpListener, state: ServerState) -> Result<()> {
    let handlers = Arc::new(Handlers::new(state));
    loop {
        let (socket, peer) = listener.accept().await?;
        let handlers = Arc::clone(&handlers);
        info!(%peer, "client connected");
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, handlers).await {
                debug!(error = %e, "connection ended");
            }
        });
    }
}
