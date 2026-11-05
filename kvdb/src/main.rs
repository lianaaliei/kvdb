use kvdb::engine::{Db, Options};
use kvdb::resp::{read_command, Reply};
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn run(db: &Db, args: &[Vec<u8>]) -> io::Result<(Reply, bool)> {
    let cmd = args[0].to_ascii_uppercase();
    let arity = |n: usize| args.len() == n;
    let wrong = |c: &[u8]| Reply::Error(format!("ERR wrong number of arguments for '{}'", String::from_utf8_lossy(c).to_lowercase()));
    let r = match cmd.as_slice() {
        b"PING" if arity(1) => Reply::Simple("PONG".into()),
        b"PING" if arity(2) => Reply::Bulk(Some(args[1].clone())),
        b"SET" if arity(3) => {
            db.put(&args[1], &args[2])?;
            Reply::Simple("OK".into())
        }
        b"GET" if arity(2) => Reply::Bulk(db.get(&args[1])?),
        b"EXISTS" if arity(2) => Reply::Int(db.get(&args[1])?.is_some() as i64),
        b"DEL" if args.len() >= 2 => {
            let mut n = 0;
            for k in &args[1..] {
                if db.get(k)?.is_some() {
                    db.delete(k)?;
                    n += 1;
                }
            }
            Reply::Int(n)
        }
        b"COMPACT" if arity(1) => {
            db.compact()?;
            Reply::Simple("OK".into())
        }
        b"FLUSH" if arity(1) => {
            db.flush()?;
            Reply::Simple("OK".into())
        }
        b"INFO" => {
            let s = db.stats();
            Reply::Bulk(Some(
                format!(
                    "memtable_entries:{}\r\nmemtable_bytes:{}\r\nsstables:{}\r\nsstable_entries:{}\r\nsstable_bytes:{}\r\n",
                    s.memtable_entries, s.memtable_bytes, s.sstables, s.sstable_entries, s.sstable_bytes
                )
                .into_bytes(),
            ))
        }
        b"COMMAND" => Reply::EmptyArray, // redis-cli handshake
        b"QUIT" => return Ok((Reply::Simple("OK".into()), true)),
        b"PING" | b"SET" | b"GET" | b"EXISTS" | b"DEL" | b"COMPACT" | b"FLUSH" => wrong(&cmd),
        _ => Reply::Error(format!("ERR unknown command '{}'", String::from_utf8_lossy(&args[0]))),
    };
    Ok((r, false))
}

fn handle(db: Arc<Db>, stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut r = BufReader::new(stream.try_clone()?);
    let mut w = BufWriter::new(stream);
    loop {
        let args = match read_command(&mut r) {
            Ok(None) => return Ok(()),
            Ok(Some(a)) => a,
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                Reply::Error(format!("ERR Protocol error: {e}")).write_to(&mut w)?;
                return w.flush();
            }
            Err(e) => return Err(e),
        };
        if args.is_empty() {
            continue;
        }
        let (reply, quit) = match run(&db, &args) {
            Ok(x) => x,
            Err(e) => (Reply::Error(format!("ERR {e}")), false),
        };
        reply.write_to(&mut w)?;
        // pipelining: only flush when the client has nothing more queued
        if quit || r.buffer().is_empty() {
            w.flush()?;
        }
        if quit {
            return Ok(());
        }
    }
}

fn main() -> io::Result<()> {
    let (mut dir, mut addr, mut opts) = ("./kvdb-data".to_string(), "127.0.0.1:6380".to_string(), Options::default());
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--dir" => dir = it.next().expect("--dir <path>"),
            "--addr" => addr = it.next().expect("--addr <host:port>"),
            "--memtable-mb" => opts.memtable_limit = it.next().and_then(|v| v.parse::<usize>().ok()).expect("--memtable-mb <n>") << 20,
            "--sync" => opts.sync = true,
            _ => {
                eprintln!("usage: kvdb [--dir PATH] [--addr HOST:PORT] [--memtable-mb N] [--sync]");
                std::process::exit(2);
            }
        }
    }
    let db = Arc::new(Db::open(&dir, opts)?);
    let listener = TcpListener::bind(&addr)?;
    eprintln!("kvdb listening on {addr}, data in {dir}");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let db = db.clone();
                thread::spawn(move || {
                    if let Err(e) = handle(db, s) {
                        if e.kind() != io::ErrorKind::ConnectionReset && e.kind() != io::ErrorKind::BrokenPipe {
                            eprintln!("connection error: {e}");
                        }
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}
