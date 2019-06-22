use std::io::{Write, Result};

use crate::protos::orc_proto;
use crate::schema::Schema;
use crate::writer::Config;
use crate::writer::count_write::CountWrite;
use crate::writer::encoder::{BooleanRLE, ByteRLE};
use crate::writer::stripe::StreamInfo;
use crate::writer::statistics::{Statistics, BaseStatistics, GenericStatistics};
use crate::writer::data::common::BaseData;
use crate::writer::data::Data;

pub struct UnionData<'a> {
    column_id: u32,
    pub(crate) schema: &'a Schema,
    pub(crate) children: Vec<Data<'a>>,
    child_counts: Vec<u64>,
    present: BooleanRLE,
    tags: ByteRLE,
    stripe_stats: GenericStatistics,
}

impl<'a> UnionData<'a> {
    pub(crate) fn new(schema: &'a Schema, config: &'a Config, column_id: &mut u32) -> Self {
        let cid = *column_id;
        let mut children: Vec<Data> = Vec::new();
        *column_id += 1;

        if let Schema::Union(fields) = schema {
            if fields.len() > 256 {
                panic!("Unions are limited to at most 256 variants");
            }
            for field in fields {
                children.push(Data::new(field, config, column_id));
            }

            Self {
                column_id: cid,
                schema,
                present: BooleanRLE::new(&config.compression),
                children: children,
                child_counts: vec![0; fields.len()],
                tags: ByteRLE::new(&config.compression),
                stripe_stats: GenericStatistics::new(),
            }
        } else { unreachable!() }
    }
    
    pub fn fields(&self) -> &'a [Schema] {
        if let Schema::Union(f) = self.schema {
            f.as_slice()
        } else { unreachable!() }
    }

    pub fn children(&mut self) -> &mut [Data<'a>] {
        &mut self.children
    }

    pub fn child(&mut self, i: usize) -> &mut Data<'a> {
        &mut self.children[i]
    }

    pub fn write(&mut self, present: bool, tag: usize) {
        if tag > self.child_counts.len() {
            panic!("tag ({}) out of range", tag);
        }
        self.present.write(present);
        self.tags.write(tag as u8);
        self.child_counts[tag] += 1;
        self.stripe_stats.update(present);
    }

    pub fn column_id(&self) -> u32 { self.column_id }
}

impl<'a> BaseData<'a> for UnionData<'a> {
    fn schema(&self) -> &'a Schema { self.schema }

    fn column_id(&self) -> u32 { self.column_id }

    fn write_index_streams<W: Write>(&mut self, _out: &mut CountWrite<W>, _stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        Ok(())
    }

    fn write_data_streams<W: Write>(&mut self, out: &mut CountWrite<W>, stream_infos_out: &mut Vec<StreamInfo>) -> Result<()> {
        if self.stripe_stats.has_null() {
            let present_start_pos = out.pos();
            self.present.finish(out)?;
            let present_len = (out.pos() - present_start_pos) as u64;
            stream_infos_out.push(StreamInfo {
                kind: orc_proto::Stream_Kind::PRESENT,
                column_id: self.column_id,
                length: present_len,
            });
        }
        
        let tags_start_pos = out.pos();
        self.tags.finish(out)?;
        let tags_len = (out.pos() - tags_start_pos) as u64;
        stream_infos_out.push(StreamInfo {
            kind: orc_proto::Stream_Kind::DATA,
            column_id: self.column_id,
            length: tags_len,
        });

        for child in &mut self.children {
            child.write_data_streams(out, stream_infos_out)?;
        }

        Ok(())
    }

    fn column_encodings(&self, out: &mut Vec<orc_proto::ColumnEncoding>) {
        assert_eq!(out.len(), self.column_id as usize);
        let mut encoding = orc_proto::ColumnEncoding::new();
        encoding.set_kind(orc_proto::ColumnEncoding_Kind::DIRECT);
        out.push(encoding);
        for child in &self.children {
            child.column_encodings(out);
        }
    }

    fn statistics(&self, out: &mut Vec<Statistics>) {
        assert_eq!(out.len(), self.column_id as usize);
        out.push(Statistics::Generic(self.stripe_stats));
        for child in &self.children {
            child.statistics(out);
        }
    }

    fn verify_row_count(&self, expected_row_count: u64) {
        let rows_written = self.stripe_stats.num_values();
        if rows_written != expected_row_count {
            panic!("In column {} (type Union), the number of values written ({}) does not match the expected number ({})", 
                self.column_id, rows_written, expected_row_count);
        }

        for (i, child) in self.children.iter().enumerate() {
            child.verify_row_count(self.child_counts[i]);
        }
    }

    fn estimated_size(&self) -> usize {
        let mut size = self.present.estimated_size() + self.tags.estimated_size();
        for child in &self.children {
            size += child.estimated_size();
        }
        size
    }

    fn reset(&mut self) {
        self.stripe_stats = GenericStatistics::new();
        self.child_counts = vec![0; self.children.len()];
        for child in &mut self.children {
            child.reset();
        }
    }
}
