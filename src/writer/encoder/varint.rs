use crate::writer::compression::{CompressionStream};

pub trait VarInt: Copy + Default {
    fn write_varint(self, out: &mut CompressionStream);
    fn wrapping_sub_i64(self, rhs: Self) -> i64;
}

impl VarInt for u64 {
    #[inline(always)]
    fn write_varint(mut self, out: &mut CompressionStream) {
        let mut buf = [0; 10];
        let mut len: usize = 0;

        for i in 0..10 {
            if self < 0x80 { buf[i] = self as u8; len = i + 1; break }
            buf[i] = 0x80 | (self as u8);
            self >>= 7;
        }
        out.write_bytes(&buf[..len]);
    }

    #[inline(always)]
    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs) as i64
    }
}

impl VarInt for i64 {
    #[inline(always)]
    fn write_varint(self, out: &mut CompressionStream) {
        let zigzag_encoding = (self << 1) ^ (self >> 63);
        (zigzag_encoding as u64).write_varint(out);
    }

    #[inline(always)]
    fn wrapping_sub_i64(self, rhs: Self) -> i64 {
        self.wrapping_sub(rhs)
    }
}
