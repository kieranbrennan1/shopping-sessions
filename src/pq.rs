// Parquet output with the parquet crate's low-level column writers (no Arrow, no compression).
use crate::{Acc, B};
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

#[derive(Default)]
pub struct CampaignRows {
    pub id: Vec<B>,
    pub channel: Vec<B>,
    pub day: Vec<i32>,
    pub sessions: Vec<i64>,
    pub purchasing: Vec<i64>,
    pub revenue: Vec<i64>,
    pub assisted: Vec<i64>,
}

enum Col<'a> {
    Str(Vec<ByteArray>, Option<&'a [i16]>),
    I32(&'a [i32]),
    I64(&'a [i64]),
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn bytes(v: &[B]) -> Vec<ByteArray> {
    v.iter().map(|s| ByteArray::from(bytes::Bytes::from_static(s))).collect()
}

fn write(path: &Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).expect("schema"));
    let file = BufWriter::with_capacity(1 << 20, File::create(path).expect("create"));
    let mut w = SerializedFileWriter::new(file, schema, props()).expect("writer");
    let mut rg = w.next_row_group().expect("row group");
    let mut it = cols.into_iter();
    while let Some(mut col) = rg.next_column().expect("column") {
        match it.next().expect("col count") {
            Col::Str(v, def) => {
                col.typed::<ByteArrayType>().write_batch(&v, def, None).expect("write");
            }
            Col::I32(v) => {
                col.typed::<Int32Type>().write_batch(v, None, None).expect("write");
            }
            Col::I64(v) => {
                col.typed::<Int64Type>().write_batch(v, None, None).expect("write");
            }
        }
        col.close().expect("close col");
    }
    rg.close().expect("close rg");
    w.close().expect("close file");
}

pub fn write_campaign(path: &Path, r: &CampaignRows) {
    write(
        path,
        "message campaign_daily { required binary campaign_id (STRING); required binary channel (STRING); \
         required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions; \
         required int64 revenue_cents; required int64 assisted_sessions; }",
        vec![
            Col::Str(bytes(&r.id), None),
            Col::Str(bytes(&r.channel), None),
            Col::I32(&r.day),
            Col::I64(&r.sessions),
            Col::I64(&r.purchasing),
            Col::I64(&r.revenue),
            Col::I64(&r.assisted),
        ],
    );
}

pub fn write_user(path: &Path, a: &Acc, cat_names: &[B]) {
    let def: Vec<i16> = a.u_top.iter().map(|t| t.is_some() as i16).collect();
    let tops: Vec<ByteArray> = a.u_top.iter().flatten().map(|&c| ByteArray::from(bytes::Bytes::from_static(cat_names[c as usize]))).collect();
    write(
        path,
        "message user_daily { required binary user_id (STRING); required int32 day (DATE); \
         required int64 sessions; required int64 events; required int64 revenue_cents; \
         optional binary top_category (STRING); }",
        vec![
            Col::Str(bytes(&a.u_name), None),
            Col::I32(&a.u_day),
            Col::I64(&a.u_sess),
            Col::I64(&a.u_events),
            Col::I64(&a.u_rev),
            Col::Str(tops, Some(&def)),
        ],
    );
}
