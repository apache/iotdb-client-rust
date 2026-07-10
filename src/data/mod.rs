//! Data structures: TSDataType codes, Tablet, SessionDataSet, BitMap.
//! Data-type codes must match the official TSFile spec (0–11), identical
//! across all IoTDB client SDKs.

/// Official TSFile data type codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TSDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float = 3,
    Double = 4,
    Text = 5,
    Vector = 6,
    Unknown = 7,
    Timestamp = 8,
    Date = 9,
    Blob = 10,
    String = 11,
}
