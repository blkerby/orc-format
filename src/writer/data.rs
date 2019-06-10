use super::encoder::{BooleanRLE, SignedIntRLEv1};
use super::super::schema::{Schema, Field};

pub struct LongData {
    present: BooleanRLE,
    data: SignedIntRLEv1,
}

impl LongData {
    pub(crate) fn new() -> Self {
        LongData {
            present: BooleanRLE::new(),
            data: SignedIntRLEv1::new(),
        }
    }

    pub fn push(&mut self, x: Option<i64>) {
        match x {
            Some(y) => {
                self.present.write(true);
                self.data.write(y);
            }
            None => { self.present.write(false); }
        }
    }
}

pub struct StructData {
    present: BooleanRLE,
    children: Vec<Data>
}

impl StructData {
    pub(crate) fn new(fields: &[Field]) -> Self {
        StructData {
            present: BooleanRLE::new(),
            children: fields.iter().map(|x| Data::new(&x.schema)).collect()
        }
    }
}

pub enum Data {
    Long(LongData),
    Struct(StructData)
}

impl Data {
    pub(crate) fn new(schema: &Schema) -> Self {
        match schema {
            Schema::Int => Data::Long(LongData::new()),
            Schema::Struct(fields) => Data::Struct(StructData::new(fields)),
        }
    }
}
