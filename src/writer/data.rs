use std::io::{Write, Result};
use enum_dispatch::enum_dispatch;

use crate::schema::{Schema, Field};
use super::encoder::{BooleanRLE, SignedIntRLEv1, UnsignedIntRLEv1};
use super::stripe::StreamInfo;

#[enum_dispatch]
pub trait BaseData<'a> {
    fn column_id(&self) -> u32;
    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize>;
    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize>;
}

pub struct LongData<'a> {
    pub(crate) column_id: u32,
    pub(crate) schema: &'a Schema,
    present: BooleanRLE,
    data: SignedIntRLEv1,
}

impl<'a> LongData<'a> {
    pub(crate) fn new(schema: &'a Schema, column_id: &mut u32) -> Self {
        let cid = *column_id;
        *column_id += 1;
        LongData {
            column_id: cid,
            schema,
            present: BooleanRLE::new(),
            data: SignedIntRLEv1::new(),
        }
    }

    pub fn write(&mut self, x: Option<i64>) {
        match x {
            Some(y) => {
                self.present.write(true);
                self.data.write(y);
            }
            None => { self.present.write(false); }
        }
    }
}

impl<'a> BaseData<'a> for LongData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize> {
        Ok(0)
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize> {
        let present_len = self.present.finish(out);


        let data_len = self.data.finish(out);
        Ok(present_len + data_len)
    }
}

pub struct StructData<'a> {
    column_id: u32,
    pub(crate) fields: &'a [Field],
    pub(crate) children: Vec<Data<'a>>,
    present: BooleanRLE,
}

impl<'a> StructData<'a> {
    pub(crate) fn new(fields: &'a [Field], column_id: &mut u32) -> Self {
        let cid = *column_id;
        let mut children: Vec<Data> = Vec::new();
        for field in fields {
            *column_id += 1;
            children.push(Data::new(&field.schema, column_id));
        }

        StructData {
            column_id: cid,
            fields,
            present: BooleanRLE::new(),
            children: children,
        }
    }

    pub fn children(&mut self) -> &mut [Data<'a>] {
        &mut self.children
    }

    pub fn write(&mut self, present: bool) {
        self.present.write(present);
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl<'a> BaseData<'a> for StructData<'a> {
    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize> {
        Ok(0)
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut W, stream_infos_out: &mut Vec<StreamInfo>) -> Result<usize> {
        Ok(0)
    }
}


#[enum_dispatch(BaseData)]
pub enum Data<'a> {
    Long(LongData<'a>),
    Struct(StructData<'a>)
}

impl<'a> Data<'a> {
    pub(crate) fn new(schema: &'a Schema, column_id: &mut u32) -> Self {
        match schema {
            Schema::Short | Schema::Int | Schema::Long => Data::Long(LongData::new(schema, column_id)),
            Schema::Struct(fields) => Data::Struct(StructData::new(fields, column_id)),
        }
    }
}
