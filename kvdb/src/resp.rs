//! Minimal RESP2 codec (enough for redis-cli, redis-benchmark and client libraries).
use std::io::{self, BufRead, Read, Write};

const MAX_LINE: u64 = 64 * 1024;
const MAX_BULK: usize = 64 * 1024 * 1024;
const MAX_ARGS: usize = 1 << 16;

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn read_line<R: BufRead>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    let n = r.by_ref().take(MAX_LINE).read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if buf.pop() != Some(b'\n') {
        return Err(invalid("line too long or truncated"));
    }
    if buf.last() == Some(&b'\r') {
        buf.pop();
    }
    Ok(Some(buf))
}

fn parse_int(b: &[u8]) -> io::Result<usize> {
    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).ok_or_else(|| invalid("bad integer"))
}

/// Reads one command. `Ok(None)` = clean EOF. Supports RESP arrays and inline (telnet) commands.
pub fn read_command<R: BufRead>(r: &mut R) -> io::Result<Option<Vec<Vec<u8>>>> {
    let line = match read_line(r)? {
        None => return Ok(None),
        Some(l) => l,
    };
    if line.first() != Some(&b'*') {
        return Ok(Some(line.split(|b| b.is_ascii_whitespace()).filter(|p| !p.is_empty()).map(|p| p.to_vec()).collect()));
    }
    let n = parse_int(&line[1..])?;
    if n > MAX_ARGS {
        return Err(invalid("too many arguments"));
    }
    let mut args = Vec::with_capacity(n.min(16));
    for _ in 0..n {
        let h = read_line(r)?.ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        if h.first() != Some(&b'$') {
            return Err(invalid("expected bulk string"));
        }
        let len = parse_int(&h[1..])?;
        if len > MAX_BULK {
            return Err(invalid("bulk string too large"));
        }
        let mut data = vec![0u8; len + 2];
        r.read_exact(&mut data)?;
        if &data[len..] != b"\r\n" {
            return Err(invalid("missing CRLF"));
        }
        data.truncate(len);
        args.push(data);
    }
    Ok(Some(args))
}

pub enum Reply {
    Simple(String),
    Error(String),
    Bulk(Option<Vec<u8>>),
    Int(i64),
    EmptyArray,
}

impl Reply {
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Reply::Simple(s) => write!(w, "+{s}\r\n"),
            Reply::Error(s) => write!(w, "-{s}\r\n"),
            Reply::Int(i) => write!(w, ":{i}\r\n"),
            Reply::EmptyArray => w.write_all(b"*0\r\n"),
            Reply::Bulk(None) => w.write_all(b"$-1\r\n"),
            Reply::Bulk(Some(b)) => {
                write!(w, "${}\r\n", b.len())?;
                w.write_all(b)?;
                w.write_all(b"\r\n")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parses_resp_and_inline() {
        let mut c = Cursor::new(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nv\n\r\nPING  hi\r\n".to_vec());
        assert_eq!(read_command(&mut c).unwrap().unwrap(), vec![b"SET".to_vec(), b"k".to_vec(), b"v\n".to_vec()]);
        assert_eq!(read_command(&mut c).unwrap().unwrap(), vec![b"PING".to_vec(), b"hi".to_vec()]);
        assert!(read_command(&mut c).unwrap().is_none());
    }

    #[test]
    fn rejects_garbage() {
        assert!(read_command(&mut Cursor::new(b"*2\r\n$99999999999\r\n".to_vec())).is_err());
        assert!(read_command(&mut Cursor::new(b"*1\r\nfoo\r\n".to_vec())).is_err());
    }
}
