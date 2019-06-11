use super::encoder::{BooleanRLE, SignedIntRLEv1};
use super::super::schema::{Schema, Field};

pub struct LongData {
    column_id: u32,
    present: BooleanRLE,
    data: SignedIntRLEv1,
}

impl LongData {
    pub(crate) fn new(column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        LongData {
            column_id: cid,
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
    column_id: u32,
    present: BooleanRLE,
    children: Vec<Data>
}

impl StructData {
    pub(crate) fn new(fields: &[Field], column_id: &mut u32) -> Self {
        let cid = *column_id;
        let mut children: Vec<Data> = Vec::new();
        for field in fields {
            *column_id += 1;
            children.push(Data::new(&field.schema, column_id));
        }

        StructData {
            column_id: cid,
            present: BooleanRLE::new(),
            children: children,
        }
    }
}

pub enum Data {
    Long(LongData),
    Struct(StructData)
}

impl Data {
    pub(crate) fn new(schema: &Schema, column_id: &mut u32) -> Self {
        match schema {
            Schema::Long => Data::Long(LongData::new(column_id)),
            Schema::Struct(fields) => Data::Struct(StructData::new(fields, column_id)),
        }
    }
}
