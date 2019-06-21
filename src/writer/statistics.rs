use crate::protos::orc_proto;

pub use common::BaseStatistics;
pub use long::LongStatistics;
pub use struct_::StructStatistics;
pub use string::StringStatistics;

mod common;
mod long;
mod struct_;
mod string;


#[derive(Debug, Clone)]
pub enum Statistics {
    Long(LongStatistics),
    Struct(StructStatistics),
    String(StringStatistics),
}

impl Statistics {
    pub fn unwrap_long(&self) -> &LongStatistics { 
        if let Statistics::Long(x) = self { x } else { panic!("invalid argument to unwrap_long"); }
    }

    pub fn unwrap_struct(&self) -> &StructStatistics { 
        if let Statistics::Struct(x) = self { x } else { panic!("invalid argument to unwrap_struct"); }
    }

    pub fn unwrap_string(&self) -> &StringStatistics { 
        if let Statistics::String(x) = self { x } else { panic!("invalid argument to unwrap_string"); }
    }

    pub fn to_proto(&self) -> orc_proto::ColumnStatistics {
        let mut stat = orc_proto::ColumnStatistics::new();
        match self {
            Statistics::Long(long_statistics) => {
                let mut int_stat = orc_proto::IntegerStatistics::new();
                if let Some(x) = long_statistics.min { int_stat.set_minimum(x); }
                if let Some(x) = long_statistics.max { int_stat.set_maximum(x); }
                if let Some(x) = long_statistics.sum { int_stat.set_sum(x); }
                stat.set_intStatistics(int_stat);
                stat.set_numberOfValues(long_statistics.num_rows);
                stat.set_hasNull(long_statistics.has_null);
            }
            Statistics::Struct(struct_statistics) => {
                stat.set_numberOfValues(struct_statistics.num_rows);
                stat.set_hasNull(struct_statistics.has_null);
            }
            Statistics::String(string_statistics) => {
                let mut str_stat = orc_proto::StringStatistics::new();
                if let Some(x) = &string_statistics.min { str_stat.set_minimum(x.clone()); }
                if let Some(x) = &string_statistics.max { str_stat.set_maximum(x.clone()); }
                str_stat.set_sum(string_statistics.sum_lengths as i64);
                stat.set_stringStatistics(str_stat);
                stat.set_numberOfValues(string_statistics.num_rows);
                stat.set_hasNull(string_statistics.has_null);
            }
        }
        stat
    }
}

impl BaseStatistics for Statistics {
    fn num_rows(&self) -> u64 {
        match self {
            Statistics::Long(x) => x.num_rows(),
            Statistics::Struct(x) => x.num_rows(),
            Statistics::String(x) => x.num_rows(),
        }
    }

    fn has_null(&self) -> bool {
        match self {
            Statistics::Long(x) => x.has_null(),
            Statistics::Struct(x) => x.has_null(),
            Statistics::String(x) => x.has_null(),
        }
    }

    fn merge(&mut self, rhs: &Statistics) {
        match self {
            Statistics::Long(x) => x.merge(rhs.unwrap_long()),
            Statistics::Struct(x) => x.merge(rhs.unwrap_struct()),
            Statistics::String(x) => x.merge(rhs.unwrap_string()),
        }
    }
}
