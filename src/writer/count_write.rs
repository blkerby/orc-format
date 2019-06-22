use std::io::{Write, Result};

pub struct CountWrite<W: Write> {
    pub inner: W,
    pub count: usize,
}

impl<W: Write> CountWrite<W> {
    pub fn new(inner: W) -> Self {
        CountWrite {
            inner: inner,
            count: 0,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    pub fn pos(&self) -> usize {
        self.count
    }
}

impl<W: Write> Write for CountWrite<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let size = self.inner.write(buf)?;
        self.count += size;
        Ok(size)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}