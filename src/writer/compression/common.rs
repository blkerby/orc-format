use crate::protos::orc_proto;
use crate::buffer::Buffer;

pub const MAX_BLOCK_SIZE: usize = 0x7fffff;

pub trait CompressionTrait {
    fn kind(&self) -> orc_proto::CompressionKind;
    fn block_size(&self) -> usize;
    fn compressor(&self) -> Option<Box<dyn Compressor>>;
}

pub trait Compressor {
    fn compress(&mut self, input: &[u8], output: &mut Buffer);
}
