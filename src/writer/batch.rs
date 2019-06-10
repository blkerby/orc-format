pub struct LongBatch {

}

impl LongBatch {
    pub fn push(x: Option<i64>) {

    }
}

// impl StringColumnBatch {
//     pub fn push(x: Option<&str>) {

//     }
// }

pub struct StructBatch {
    children: Vec<DataBatch>
}

impl StructBatch {
    pub fn children(&mut self) -> &mut [DataBatch] {
        self.children.as_mut_slice()
    }
}

pub struct Batch<'a, 'b, W: Write> {
    batch_writer: &'b mut BatchWriter<'a, W>
}


#[must_use]
pub struct BatchWriter<'a, W: Write> {
    writer: Writer<'a, W>
}


impl BatchWriter {
    pub finish(self) -> Writer<'a, W: Write> {
        // TODO: actually write stuff here.
        self.writer
    }

    pub fn batch(&mut self) -> &mut Batch {
        &mut self.data_batch
    }
}


pub enum Batch {
    Long(LongBatch),
    // String(StringColumnBatch)
    Struct(StructBatch),
}
