use crate::protos::orc_proto;
use super::common::{CompressionTrait, Compressor};

// Include a private dummy field inside, to prevent external construction of the struct
// except through the public API. This leaves open the possibility for us to add additional
// fields later in a backward-compatible way.
#[derive(Clone)]
pub struct NoCompression {
    _dummy: (),
}

impl NoCompression {
    pub fn new() -> NoCompression {
        NoCompression { _dummy: () }
    }
}

impl CompressionTrait for NoCompression {
    fn kind(&self) -> orc_proto::CompressionKind {
        orc_proto::CompressionKind::NONE
    }

    fn block_size(&self) -> usize {
        0
    }

    fn compressor(&self) -> Option<Box<dyn Compressor>> {
        None
    }
}
