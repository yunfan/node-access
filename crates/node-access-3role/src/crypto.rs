use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct FrameCrypto {
    cipher: Arc<Aes256Gcm>,
    outbound_dir: u8,
    inbound_dir: u8,
    outbound_counter: Arc<AtomicU64>,
}

impl FrameCrypto {
    pub fn from_secret(secret: &str, outbound_dir: u8, inbound_dir: u8) -> Self {
        let key = Sha256::digest(secret.as_bytes());
        Self {
            cipher: Arc::new(Aes256Gcm::new_from_slice(&key).expect("sha256 is 32 bytes")),
            outbound_dir,
            inbound_dir,
            outbound_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn seal(&self, plaintext: &[u8]) -> Result<(u64, Vec<u8>)> {
        let counter = self.outbound_counter.fetch_add(1, Ordering::Relaxed);
        let nonce = make_nonce(self.outbound_dir, counter);
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(|_| anyhow!("failed to encrypt relay frame"))?;
        Ok((counter, ciphertext))
    }

    pub fn open(&self, counter: u64, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let nonce = make_nonce(self.inbound_dir, counter);
        self.cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext)
            .map_err(|_| anyhow!("failed to decrypt relay frame"))
            .with_context(|| "check that both sides use the same --secret")
    }
}

fn make_nonce(direction: u8, counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0] = direction;
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}
