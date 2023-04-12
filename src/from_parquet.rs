use bigdecimal::{BigDecimal, ToPrimitive};
use bytes::Bytes;
use chrono::{DateTime, Duration, FixedOffset};
use nu_protocol::{ShellError, Span, Value};
use num_bigint::BigInt;
use num_rational::BigRational;
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

fn parquet_decimal_to_bigrational(decimal: &Decimal) -> BigRational {
    let unscaled_value = BigInt::from_signed_bytes_be(decimal.data());
    // Should never be negative per-spec, so we just act like it's not. If it does happen to be
    // negative, we just accept that the value will wrap around, since it's already breaking the
    // spec.
    let scale = decimal.scale() as u32;
    BigRational::new(unscaled_value, BigInt::from(10).pow(scale))
}

fn parquet_decimal_to_value(decimal: &Decimal, span: Span, opts: &FromParquetOpts) -> Value {
    const DECIMAL_HELP: &'static str = "cannot convert decimal to float.";

    let f64 = if opts.rational {
        let rational = parquet_decimal_to_bigrational(decimal);
        if opts.extended_decimal {
            let value = rational
                .to_f64()
                .map(|f| Value::float(f, span))
                .unwrap_or_else(|| Value::nothing(span));

            let numerator = rational
                .numer()
                .to_i64()
                .map(|i| Value::int(i, span))
                .unwrap_or_else(|| Value::nothing(span));

            let denominator = rational
                .denom()
                .to_i64()
                .map(|i| Value::int(i, span))
                .unwrap_or_else(|| Value::nothing(span));

            let text = Value::string(rational.to_string(), span);

            return Value::record(
                vec![
                    format!("value"),
                    format!("numerator"),
                    format!("denominator"),
                    format!("text"),
                ],
                vec![value, numerator, denominator, text],
                span,
            );
        }
        rational.to_f64()
    } else {
        let decimal = parquet_decimal_to_bigdecimal(decimal);
        if opts.extended_decimal {
            let value = decimal
                .to_f64()
                .map(|f64| Value::float(f64, span))
                .unwrap_or_else(|| Value::nothing(span));
            return Value::record(
                vec![String::from("value"), String::from("text")],
                vec![value, Value::string(decimal.to_string(), span)],
                span,
            );
        }
        decimal.to_f64()
    };
    f64.map(|f| Value::float(f, span))
        .unwrap_or_else(|| Value::Error {
            error: Box::new(ShellError::CantConvert {
                to_type: "f64".into(),
                from_type: "decimal".into(),
                span,
                help: Some(String::from(DECIMAL_HELP)),
            }),
        })
}

fn convert_to_nu(field: &Field, span: Span, opts: &FromParquetOpts) -> Value {
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
        Field::Decimal(d) => parquet_decimal_to_value(d, span, opts),
        Field::Group(row) => convert_parquet_row(row, span, opts),
        Field::ListInternal(list) => Value::list(
            list.elements()
                .iter()
                .map(|v| convert_to_nu(&v, span, opts))
                .collect(),
            span,
        ),
        Field::MapInternal(map) => Value::list(
            map.entries()
                .iter()
                .map(|(k, v)| {
                    Value::list(
                        vec![convert_to_nu(k, span, opts), convert_to_nu(v, span, opts)],
                        span,
                    )
                })
                .collect::<Vec<_>>(),
            span,
        ),
    }
}

fn convert_parquet_row(row: &Row, span: Span, opts: &FromParquetOpts) -> Value {
    let mut cols = vec![];
    let mut vals = vec![];
    for (name, field) in row.get_column_iter() {
        cols.push(name.clone());
        vals.push(convert_to_nu(field, span.clone(), opts));
    }
    Value::Record { cols, vals, span }
}

#[derive(Debug, Clone, Copy)]
pub struct FromParquetOpts {
    /// Uses a [`Value::Record`] to represent the decimal value instead of a [`Value::Float`].
    ///
    /// This means that the decimal value will be represented as a record with two or three fields
    /// depending on the whether [`FromParquetOpts::rational`] is set or not.
    ///
    /// The fields are:
    /// - For BigDecimal
    ///   - `value`: The decimal value as an approximated float value.
    ///   - `text`: The decimal value as a string (integrity preserved).
    /// - For BigRational
    ///   - `numerator`: The numerator of the decimal value as an i64.
    ///   - `denominator`: The denominator of the decimal value as an i64.
    ///   - `text`: The decimal value as a string (integrity preserved).
    pub extended_decimal: bool,
    /// Uses [`BigRational`] instead of [`BigDecimal`] for decimal values.
    pub rational: bool,
}

pub fn from_parquet_bytes(bytes: Vec<u8>, span: Span, opts: &FromParquetOpts) -> Value {
    let cursor = Bytes::from(bytes);
    let reader = SerializedFileReader::new(cursor).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    let mut vals = Vec::new();
    while let Some(record) = iter.next() {
        let row = convert_parquet_row(&record, span, opts);
        vals.push(row);
    }
    Value::List { vals, span }
}
