//! 端到端协议测试:启动服务 → 原生 TCP 走 PG 协议(startup + simple query)
//! → 校验 RowDescription/DataRow/CommandComplete/ErrorResponse。
//! 等价于 psql 的最小工作路径,不依赖本机安装 psql。
//! 建表/插表/查询完整链路见 engine.rs 集成测试。

use common::config::Config;
use server::engine::Engine;
use server::handlers::ServerState;
use server::run_listener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_server() -> std::net::SocketAddr {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.memory.query_limit_bytes = 64 << 20;
    let engine = Engine::open(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // tempdir 生命周期由本任务持有
        let _dir = dir;
        run_listener(listener, ServerState { engine })
            .await
            .unwrap();
    });
    addr
}

/// 消息读取器:逐条解析 backend 消息。
struct MessageReader {
    stream: TcpStream,
}

#[derive(Debug)]
struct Message {
    tag: u8,
    payload: Vec<u8>,
}

impl Message {
    fn strings(&self) -> Vec<String> {
        self.payload
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect()
    }
}

impl MessageReader {
    async fn new(addr: std::net::SocketAddr, user: &str) -> Self {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // StartupMessage: len | 196608 | k\v pairs | \0
        let mut startup = Vec::new();
        startup.extend_from_slice(&196608i32.to_be_bytes());
        startup.extend_from_slice(b"user\0");
        startup.extend_from_slice(user.as_bytes());
        startup.push(0);
        startup.push(0);
        let len = (startup.len() + 4) as i32;
        let mut frame = Vec::new();
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&startup);
        stream.write_all(&frame).await.unwrap();
        Self { stream }
    }

    async fn read_message(&mut self) -> Message {
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).await.unwrap();
        let tag = header[0];
        let len = i32::from_be_bytes(header[1..5].try_into().unwrap()) as usize - 4;
        let mut payload = vec![0u8; len];
        if len > 0 {
            self.stream.read_exact(&mut payload).await.unwrap();
        }
        Message { tag, payload }
    }

    /// 读到 ReadyForQuery 为止,返回途中的消息。
    async fn read_until_ready(&mut self) -> Vec<Message> {
        let mut out = Vec::new();
        loop {
            let m = self.read_message().await;
            let done = m.tag == b'Z';
            out.push(m);
            if done {
                return out;
            }
        }
    }

    async fn send_query(&mut self, sql: &str) {
        let mut frame = Vec::new();
        frame.push(b'Q');
        let len = sql.len() + 4 + 1;
        frame.extend_from_slice(&(len as i32).to_be_bytes());
        frame.extend_from_slice(sql.as_bytes());
        frame.push(0);
        self.stream.write_all(&frame).await.unwrap();
    }
}

#[tokio::test]
async fn select_1_end_to_end() {
    let addr = start_server().await;
    let mut client = MessageReader::new(addr, "postgres").await;

    // startup: AuthenticationOk(R) / ParameterStatus(S) / BackendKeyData(K) / ReadyForQuery(Z)
    let startup_msgs = client.read_until_ready().await;
    assert!(
        startup_msgs.iter().any(|m| m.tag == b'R'),
        "应有 AuthenticationOk: {:?}",
        startup_msgs
            .iter()
            .map(|m| m.tag as char)
            .collect::<Vec<_>>()
    );
    assert!(startup_msgs.last().unwrap().tag == b'Z');

    // SELECT
    client.send_query("SELECT 1 + 1 AS two;").await;
    let msgs = client.read_until_ready().await;
    let tags: Vec<char> = msgs.iter().map(|m| m.tag as char).collect();
    // RowDescription T → DataRow D → CommandComplete C → ReadyForQuery Z
    assert_eq!(tags, vec!['T', 'D', 'C', 'Z'], "实际消息序列: {tags:?}");

    let row_desc = &msgs[0];
    // payload: i16 列数 + 每字段 name\0 ...
    assert_eq!(&row_desc.payload[2..6], b"two\0");

    // DataRow: i16 列数 + [len i32]["2"]
    let row = &msgs[1].payload;
    assert_eq!(u16::from_be_bytes(row[0..2].try_into().unwrap()), 1);
    let field_len = i32::from_be_bytes(row[2..6].try_into().unwrap()) as usize;
    assert_eq!(&row[6..6 + field_len], b"2");

    let complete = &msgs[2];
    assert!(complete.strings()[0].starts_with("SELECT"));
}

#[tokio::test]
async fn multiple_statements_and_error_path() {
    let addr = start_server().await;
    let mut client = MessageReader::new(addr, "postgres").await;
    let _ = client.read_until_ready().await;

    // 不存在的表 → ErrorResponse(42P01 未定义表)+ ReadyForQuery(连接仍可用)
    client.send_query("SELECT * FROM nonexistent_table;").await;
    let msgs = client.read_until_ready().await;
    let tags: Vec<char> = msgs.iter().map(|m| m.tag as char).collect();
    assert_eq!(tags, vec!['E', 'Z'], "错误后应恢复 ReadyForQuery: {tags:?}");
    let err_text = String::from_utf8_lossy(&msgs[0].payload);
    assert!(
        err_text.contains("not found")
            || err_text.contains("not implemented")
            || err_text.contains("not supported"),
        "错误信息应可读: {err_text}"
    );

    // 错误之后连接仍可执行正常查询
    client.send_query("SELECT 42;").await;
    let msgs = client.read_until_ready().await;
    let tags: Vec<char> = msgs.iter().map(|m| m.tag as char).collect();
    assert_eq!(tags, vec!['T', 'D', 'C', 'Z']);
}

#[tokio::test]
async fn empty_query_response() {
    let addr = start_server().await;
    let mut client = MessageReader::new(addr, "postgres").await;
    let _ = client.read_until_ready().await;
    client.send_query(";").await;
    let msgs = client.read_until_ready().await;
    let tags: Vec<char> = msgs.iter().map(|m| m.tag as char).collect();
    assert_eq!(
        tags,
        vec!['I', 'Z'],
        "空查询应回 EmptyQueryResponse(I): {tags:?}"
    );
}

#[tokio::test]
async fn null_and_types_roundtrip() {
    let addr = start_server().await;
    let mut client = MessageReader::new(addr, "postgres").await;
    let _ = client.read_until_ready().await;
    client.send_query("SELECT NULL WHERE 1 = 1;").await;
    let msgs = client.read_until_ready().await;
    let tags: Vec<char> = msgs.iter().map(|m| m.tag as char).collect();
    assert_eq!(tags, vec!['T', 'D', 'C', 'Z']);
    // NULL 字段在 PG 文本协议里是长度 -1;DataRow 以 i16 列数开头
    let row = &msgs[1].payload;
    assert_eq!(i32::from_be_bytes(row[2..6].try_into().unwrap()), -1);
}
