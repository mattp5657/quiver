#[derive(Debug)]
pub enum Value {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Boolean(bool),
    Date32(i32),
    TimestampMicros(i64),
    Null,
}