use std::io::Write;

use super::Buffer;

pub struct ByteRLE {
    buf: Buffer
}

impl ByteRLE {
    pub fn new() -> Self {
        ByteRLE {
            buf: Buffer::new()
        }
    }

    pub fn write(&mut self, x: u8) {
        unimplemented!();
    }

    pub fn flush<W: Write>(&mut self, out: &mut W) {
        unimplemented!();
        // self.buf.flush(out);
    }
}

pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8
}

impl BooleanRLE {
    pub fn new() -> Self {
        BooleanRLE {
            byte_rle: ByteRLE::new(),
            buf: 0,
            cnt: 0,
        }
    }

    pub fn write(&mut self, x: bool) {
        self.buf = self.buf << 1 | (x as u8);
        if self.cnt == 7 {
            self.cnt = 0;
            self.byte_rle.write(self.buf);
        } else {
            self.cnt += 1;
        }
    }

    pub fn flush<W: Write>(&mut self, out: &mut W) {
        if self.cnt > 0 {
            self.byte_rle.write(self.buf << (8 - self.cnt));
        }
        self.byte_rle.flush(out);
    }
}

pub struct SignedIntRLEv1 {
    buf: Buffer
}

impl SignedIntRLEv1 {
    pub fn new() -> Self {
        SignedIntRLEv1 {
            buf: Buffer::new(),
        }
    }

    pub fn write(&mut self, x: i64) {
        unimplemented!();
    }

    pub fn flush<W: Write>(&mut self, out: &mut W) {
        unimplemented!();
        // self.buf.flush(out);
    }
}

// pub struct UnsignedIntRLEv1 {

// }

