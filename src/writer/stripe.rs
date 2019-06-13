use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;

use super::data::Data;
use super::statistics::Statistics;


pub struct StripeInfo {
    pub offset: u64,
    pub num_rows: u64,
    pub index_length: u64,
    pub data_length: u64,
    pub footer_length: u64,
    pub statistics: Statistics,
}

pub struct StreamInfo {
    pub kind: orc_proto::Stream_Kind,
    pub column_id: u32,
    pub length: u64,
}

pub struct Stripe<'a> {
    pub data: Data<'a>,
    pub offset: u64,
    pub num_rows: u64,
}

impl<'a> Stripe<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Stripe {
            data: Data::new(schema, &mut 0),
            offset: 3,
            num_rows: 0,
        }
    }

    pub fn write_batch(&mut self, num_rows: u64) -> Result<()> {
        self.num_rows += num_rows;
        Ok(())
    }

    fn write_footer(&mut self) -> Result<u64> {
        Ok(0)
    }

    pub fn finish<W: Write>(&mut self, out: &mut W) -> Result<Option<StripeInfo>> {
        if self.num_rows == 0 { return Ok(None) }
        let mut stream_infos: Vec<StreamInfo> = Vec::new();
        // let statistics = self.data.statistics();
        // let index_length = self.data.write_index_streams(&mut out, &mut stream_infos)?;
        // let data_length = self.data.write_data_streams(&mut out, &mut stream_infos)?;
        // self.data.reset();
        let footer_length = self.write_footer()?;
        // Ok(Some(StripeInfo {
        //     offset: self.offset,
        //     num_rows: self.num_rows,
        //     index_length,
        //     data_length,
        //     footer_length,
        //     statistics,
        // })
        Ok(None)
    }


}