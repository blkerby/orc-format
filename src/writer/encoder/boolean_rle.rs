use crate::writer::compression::Compression;
use std::io::{Write, Result};

use crate::writer::encoder::byte_rle::ByteRLE;


pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8,
}

impl BooleanRLE {
    pub fn new(compression: &Compression) -> Self {
        BooleanRLE {
            byte_rle: ByteRLE::new(compression),
            buf: 0,
            cnt: 0,
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

    pub fn finish<W: Write>(&mut self, out: &mut W) -> Result<u64> {
        if self.cnt > 0 {
            self.byte_rle.write(self.buf << (8 - self.cnt));
        }
        self.byte_rle.finish(out)
    }

    pub fn estimated_size(&self) -> usize {
        self.byte_rle.estimated_size()
    }
}

