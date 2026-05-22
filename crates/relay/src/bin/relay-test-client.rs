use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .ok_or("missing mode: control | server-data | client")?;

    let mut base_url = "ws://127.0.0.1:8787".to_string();
    let mut server_id = None;
    let mut connection_id = None;
    let mut send_text = None;
    let mut version = "2".to_string();
    let mut receive_count = 1usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--base-url" => base_url = args.next().ok_or("missing --base-url value")?,
            "--server-id" => server_id = Some(args.next().ok_or("missing --server-id value")?),
            "--connection-id" => {
                connection_id = Some(args.next().ok_or("missing --connection-id value")?)
            }
            "--send-text" => send_text = Some(args.next().ok_or("missing --send-text value")?),
            "--version" => version = args.next().ok_or("missing --version value")?,
            "--receive-count" => {
                receive_count = args
                    .next()
                    .ok_or("missing --receive-count value")?
                    .parse()?
            }
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }

    let server_id = server_id.ok_or("missing --server-id")?;
    let role = match mode.as_str() {
        "control" => "server",
        "server-data" => "server",
        "client" => "client",
        _ => return Err("mode must be one of: control | server-data | client".into()),
    };

    let mut url = format!("{base_url}/ws?serverId={server_id}&role={role}&v={version}");
    if mode == "server-data" || connection_id.is_some() {
        let cid = connection_id.ok_or("missing --connection-id")?;
        url.push_str("&connectionId=");
        url.push_str(&cid);
    }

    let (mut ws, _) = connect_async(url).await?;

    if let Some(text) = send_text {
        ws.send(Message::Text(text)).await?;
    }

    for _ in 0..receive_count {
        let result = timeout(Duration::from_secs(30), ws.next()).await?;
        match result {
            Some(Ok(Message::Text(text))) => println!("{text}"),
            Some(Ok(Message::Binary(bytes))) => println!("{bytes:?}"),
            Some(Ok(Message::Close(frame))) => {
                println!("closed: {frame:?}");
                break;
            }
            Some(Ok(other)) => println!("{other:?}"),
            Some(Err(error)) => return Err(error.into()),
            None => break,
        }
    }

    Ok(())
}
