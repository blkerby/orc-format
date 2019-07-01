use crate::writer::compression::Compression;
use std::io::{Write, Result};

use crate::writer::encoder::byte_rle::{ByteRLE, ByteRLEPosition};


pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8,
}

#[derive(Copy, Clone)]
pub struct BooleanRLEPosition {
    // Position in the underlying ByteRLE
    inner: ByteRLEPosition,
    // Bit position (between 0 and 7) within the byte
    bits: u8,
}

impl BooleanRLEPosition {
    pub fn record(&self, out: &mut Vec<u64>) {
        self.inner.record(out);
        out.push(self.bits as u64);
    }
}

impl BooleanRLE {
    pub fn new(compression: &Compression) -> Self {
        BooleanRLE {
            byte_rle: ByteRLE::new(compression),
            buf: 0,
            cnt: 0,
        }
    }

    pub fn position(&self) -> BooleanRLEPosition {
        BooleanRLEPosition {
            inner: self.byte_rle.position(),
            bits: self.cnt,
        }
    }

    #[inline(always)]
    pub fn write(&mut self, x: bool) {
        self.buf = self.buf << 1 | (x as u8);
        if self.cnt == 7 {
            self.cnt = 0;
            self.byte_rle.write(self.buf);
        } else {
            self.cnt += 1;
        }
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> Result<()> {
        if self.cnt > 0 {
            self.byte_rle.write(self.buf << (8 - self.cnt));
            self.cnt = 0;
        }
        self.byte_rle.finish(out)
    }

    pub fn estimated_size(&self) -> usize {
        self.byte_rle.estimated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::NoCompression;

    #[test]
    fn test_boolean_rle() {
        let cases = vec![
            (vec![], vec![]),
            (vec![true, false, true, false, true, false, false, false, true], vec![254, 0b10101000, 0b10000000]),
            (vec![false; 80], vec![7, 0]),
        ];
        let mut rle = BooleanRLE::new(&NoCompression::new().build());
        for (input, expected_output) in cases {
            for x in input {
                rle.write(x);
            }
            let mut out: Vec<u8> = Vec::new();
            rle.finish(&mut out).unwrap();
            assert_eq!(out, expected_output);
        }
    }
}
