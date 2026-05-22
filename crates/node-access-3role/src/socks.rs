use anyhow::{anyhow, Context, Result};
use std::net::{Ipv4Addr, Ipv6Addr};
use tokio::io::{copy_bidirectional, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn serve_socks5<S>(mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).await?;
    if head[0] != 0x05 {
        return Err(anyhow!("unsupported socks version {}", head[0]));
    }

    let mut methods = vec![0u8; head[1] as usize];
    stream.read_exact(&mut methods).await?;
    stream.write_all(&[0x05, 0x00]).await?;

    let mut req = [0u8; 4];
    stream.read_exact(&mut req).await?;
    if req[0] != 0x05 {
        return Err(anyhow!("unsupported socks request version {}", req[0]));
    }
    if req[1] != 0x01 {
        send_reply(&mut stream, 0x07).await?;
        return Err(anyhow!("only socks CONNECT is supported"));
    }

    let host = match req[3] {
        0x01 => {
            let mut addr = [0u8; 4];
            stream.read_exact(&mut addr).await?;
            Ipv4Addr::from(addr).to_string()
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut name = vec![0u8; len[0] as usize];
            stream.read_exact(&mut name).await?;
            String::from_utf8(name).context("domain name is not utf-8")?
        }
        0x04 => {
            let mut addr = [0u8; 16];
            stream.read_exact(&mut addr).await?;
            Ipv6Addr::from(addr).to_string()
        }
        atyp => {
            send_reply(&mut stream, 0x08).await?;
            return Err(anyhow!("unsupported socks address type {atyp}"));
        }
    };

    let mut port_bytes = [0u8; 2];
    stream.read_exact(&mut port_bytes).await?;
    let port = u16::from_be_bytes(port_bytes);
    let target = format!("{host}:{port}");

    let mut remote = match TcpStream::connect(&target).await {
        Ok(remote) => remote,
        Err(error) => {
            let _ = send_reply(&mut stream, 0x05).await;
            return Err(error).with_context(|| format!("failed to connect {target}"));
        }
    };

    send_reply(&mut stream, 0x00).await?;
    let _ = copy_bidirectional(&mut stream, &mut remote).await?;
    Ok(())
}

async fn send_reply<S>(stream: &mut S, code: u8) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream
        .write_all(&[0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    Ok(())
}
