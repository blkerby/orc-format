use crate::writer::compression::Compression;
use std::io::{Write, Result};
use std::mem;

use crate::writer::encoder::byte_rle::{ByteRLE, ByteRLEPosition};


pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8,
    row_group_start_bits: u8,
}

pub struct BooleanRLEPosition {
    byte_rle_pos: ByteRLEPosition,
    bit_pos: u8,
}

impl BooleanRLE {
    pub fn new(compression: &Compression) -> Self {
        BooleanRLE {
            byte_rle: ByteRLE::new(compression),
            buf: 0,
            cnt: 0,
            row_group_start_bits: 0,
        }
    }

    pub fn finish_row_group(&mut self) -> BooleanRLEPosition {
        let b = mem::replace(&mut self.row_group_start_bits, self.cnt);
        BooleanRLEPosition {
            byte_rle_pos: self.byte_rle.finish_row_group(),
            bit_pos: b,
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

