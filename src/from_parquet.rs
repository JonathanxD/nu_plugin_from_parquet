use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use bytes::Bytes;
use chrono::{DateTime, Duration, FixedOffset};
use nu_protocol::{ShellError, Span, Value};
use num_bigint::BigInt;
use parquet::data_type::Decimal;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::{Field, Row};
use std::convert::TryInto;
use std::ops::Add;

fn parquet_decimal_to_bigdecimal(decimal: &Decimal) -> BigDecimal {
    let unscaled_value = BigInt::from_signed_bytes_be(decimal.data());
    BigDecimal::new(unscaled_value, decimal.scale() as i64)
}

fn convert_to_nu(field: &Field, span: Span) -> Value {
    const DECIMAL_MAX_CONVERSION: u64 = 2u64.pow(53);
    const DECIMAL_HELP: &'static str = "Currently cannot convert decimals larger than 2^53 to float.";

    let epoch: DateTime<FixedOffset> = DateTime::default();

    match field {
        Field::Null => Value::nothing(span),
        Field::Bool(b) => Value::boolean(*b, span),
        Field::Byte(b) => Value::binary(vec![*b as u8], span),
        Field::UByte(b) => Value::binary(vec![*b], span),
        Field::Short(s) => Value::int((*s).into(), span),
        Field::UShort(s) => Value::int((*s).into(), span),
        Field::Int(i) => Value::int((*i).into(), span),
        Field::UInt(i) => Value::int((*i).into(), span),
        Field::Long(l) => Value::int(*l, span),
        Field::ULong(l) => (*l)
            .try_into()
            .map(|l| Value::int(l, span))
            .unwrap_or_else(|e| Value::Error {
                error: Box::new(ShellError::CantConvert {
                    to_type: "i64".into(),
                    from_type: "u64".into(),
                    span,
                    help: Some(e.to_string()),
                }),
            }),
        Field::Float(f) => Value::float((*f).into(), span),
        Field::Double(f) => Value::float(*f, span),
        Field::Str(s) => Value::string(s, span),
        Field::Bytes(bytes) => Value::binary(bytes.data().to_vec(), span),
        Field::Date(days_since_epoch) => {
            let val = epoch.add(Duration::days(*days_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMillis(millis_since_epoch) => {
            let val = epoch.add(Duration::milliseconds(*millis_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMicros(micros_since_epoch) => {
            let val = epoch.add(Duration::microseconds(*micros_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::Decimal(d) => {
            let decimal = parquet_decimal_to_bigdecimal(&d);
            let (bigint, _) = decimal.as_bigint_and_exponent();

            if bigint > BigInt::from_u64(DECIMAL_MAX_CONVERSION).unwrap() {
                None
            } else {
                decimal.to_f64()
            }
            .map(|f| Value::float(f, span))
            .unwrap_or_else(|| Value::Error {
                error: Box::new(ShellError::CantConvert {
                    to_type: "f64".into(),
                    from_type: "decimal".into(),
                    span,
                    help: Some(String::from(DECIMAL_HELP)),
                }),
            })
        }
        Field::Group(_row) => {
            unimplemented!("Nested structs not supported yet")
        }
        Field::ListInternal(_list) => {
            unimplemented!("Lists not supported yet")
        }
        Field::MapInternal(_map) => {
            unimplemented!("Maps not supported yet")
        }
    }
}

fn convert_parquet_row(row: Row, span: Span) -> Value {
    let mut cols = vec![];
    let mut vals = vec![];
    for (name, field) in row.get_column_iter() {
        cols.push(name.clone());
        vals.push(convert_to_nu(field, span.clone()));
    }
    Value::Record { cols, vals, span }
}

pub fn from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Value {
    let cursor = Bytes::from(bytes);
    let reader = SerializedFileReader::new(cursor).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let mut vals = Vec::new();
    while let Some(record) = iter.next() {
        let row = convert_parquet_row(record, span);
        vals.push(row);
    }
    Value::List { vals, span }
}
