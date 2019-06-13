
pub struct LongStatistics {
    has_null: bool,
    min: Option<i64>,
    max: Option<i64>,
    sum: Option<i64>,
}

pub struct StructStatistics {
    has_null: bool,
}

pub enum Statistics {
    Long(LongStatistics),
    Struct(StructStatistics),
}
