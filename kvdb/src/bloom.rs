/// Bloom filter with double hashing (Kirsch-Mitzenmacher), ~1% false positives.
pub struct Bloom {
    bits: Vec<u8>,
    k: u32,
}

const BITS_PER_KEY: usize = 10;

fn fnv(key: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^ (h >> 32)
}

impl Bloom {
    pub fn with_capacity(n: usize) -> Self {
        let nbits = (n.max(1) * BITS_PER_KEY).max(64);
        Bloom { bits: vec![0; (nbits + 7) / 8], k: 7 }
    }

    fn nbits(&self) -> u64 {
        (self.bits.len() * 8) as u64
    }

    fn hashes(key: &[u8]) -> (u64, u64) {
        (fnv(key, 0xcbf2_9ce4_8422_2325), fnv(key, 0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hashes(key);
        let n = self.nbits();
        for i in 0..self.k as u64 {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % n;
            self.bits[(pos / 8) as usize] |= 1 << (pos % 8);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::hashes(key);
        let n = self.nbits();
        (0..self.k as u64).all(|i| {
            let pos = h1.wrapping_add(i.wrapping_mul(h2)) % n;
            self.bits[(pos / 8) as usize] & (1 << (pos % 8)) != 0
        })
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
    }

    pub fn decode(buf: &[u8]) -> Option<Bloom> {
        if buf.len() < 8 {
            return None;
        }
        let k = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        let n = u32::from_le_bytes(buf[4..8].try_into().ok()?) as usize;
        if k == 0 || n == 0 || buf.len() != 8 + n {
            return None;
        }
        Some(Bloom { bits: buf[8..].to_vec(), k })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_fpr() {
        let mut b = Bloom::with_capacity(10_000);
        for i in 0..10_000u32 {
            b.insert(&i.to_le_bytes());
        }
        assert!((0..10_000u32).all(|i| b.may_contain(&i.to_le_bytes())));
        let fp = (10_000..110_000u32).filter(|i| b.may_contain(&i.to_le_bytes())).count();
        assert!(fp < 3_000, "false positive rate too high: {fp}/100000");
        let mut enc = Vec::new();
        b.encode(&mut enc);
        let d = Bloom::decode(&enc).unwrap();
        assert!(d.may_contain(&42u32.to_le_bytes()));
    }
}
