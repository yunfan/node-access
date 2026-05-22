use anyhow::Result;
use tokio::io::{copy_bidirectional, AsyncRead, AsyncWrite};

pub async fn pipe<S, T>(mut left: S, mut right: T) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let _ = copy_bidirectional(&mut left, &mut right).await?;
    Ok(())
}
