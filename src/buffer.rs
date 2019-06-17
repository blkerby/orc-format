use std::ops::{Deref, DerefMut};

/// Simple wrapper around a Vec<u8>, which we use for all our substantial memory allocations.
/// The encapsulation here should make it easier to later swap in a different approach to 
/// allocation if we want to.
pub struct Buffer {
    data: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Buffer {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Buffer {
        Buffer {
            data: Vec::with_capacity(capacity)
        }
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn resize(&mut self, new_len: usize) {
        self.data.resize(new_len, 0);
    }

    pub fn ensure_size(&mut self, size: usize) {
        if self.data.len() < size {
            self.resize(size);
        }
    }

    pub fn write_u8(&mut self, b: u8) {
        self.data.push(b);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.data.extend(bytes);
    }
}

impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
