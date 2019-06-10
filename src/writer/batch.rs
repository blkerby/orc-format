use std::io::Write;

use super::Writer;
use super::data::{Data, LongData, StructData};

pub struct LongBatch<'a>(&'a mut LongData);

impl<'a> LongBatch<'a> {
    pub fn push(&mut self, x: Option<i64>) {
        match x {
            Some(y) => {
                self.0.present.write(true);
                self.0.data.write(y);
            }
            None => { self.0.present.write(false); }
        }
    }
}

// impl StringColumnBatch {
//     pub fn push(x: Option<&str>) {

//     }
// }

fn data_batch(data: &mut Data) -> Batch {
    match data {
        Data::Long(data) => Batch::Long(LongBatch(data)),
        Data::Struct(data) => Batch::Struct(StructBatch(data)),
    }
}

pub struct StructBatch<'a>(&'a mut StructData);

impl<'a> StructBatch<'a> {
    pub fn child(&mut self, i: usize) -> &mut Batch<'a> {
        data_batch(self.0.children[i])
    }

    // pub fn children(&mut self) -> impl Iterator<Item=&mut Batch<'a, 'b, W>> {
    // }
}

#[must_use]
pub struct BatchWriter<'a, W: Write> {
    writer: Writer<'a, W>
}


impl<'a, W: Write> BatchWriter<'a, W> {
    pub fn finish(self) -> Writer<'a, W> {
        // TODO: actually write stuff here.
        self.writer
    }

    pub fn data(&mut self) -> &mut Data {
        &mut self.writer.data
    }
}


pub enum Batch<'a> {
    Long(LongBatch<'a>),
    Struct(StructBatch<'a>),
}
