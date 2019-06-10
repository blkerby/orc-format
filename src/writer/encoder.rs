use super::Buffer;

pub struct ByteRLE {
    buf: Buffer
}

pub struct BooleanRLE {
    byte_rle: ByteRLE,
    buf: u8,
    cnt: u8
}

impl BooleanRLE {
    fn write(&mut self, x: bool) {
        self.buf = self.buf << 1 | x;
        if self.cnt == 7 {
            self.cnt = 0;
            self.byte_rle.write(self.buf);
        } else {
            self.cnt += 1;
        }
    }

    fn flush(&mut self) {
        if self.cnt > 0 {
            self.byte_rle.write(self.buf << (8 - self.cnt));
        }        
    }
}

pub struct SignedIntRLEv1 {
}

pub struct UnsignedIntRLEv1 {

}

