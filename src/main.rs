mod from_parquet;

use nu_plugin::{serve_plugin, EvaluatedCall, JsonSerializer, LabeledError, Plugin};
use nu_protocol::{PluginSignature, Value};

use crate::from_parquet::FromParquetOpts;

struct FromParquet;

impl FromParquet {
    const EXTENDED_FORMAT_OPTION: &'static str = "extended-decimal";
    const RATIONAL_OPTION: &'static str = "rational";

    fn new() -> Self {
        Self {}
    }
}

impl Plugin for FromParquet {
    fn signature(&self) -> Vec<PluginSignature> {
        vec![
            PluginSignature::build("from parquet")
            .usage("Convert from .parquet binary into table")
            .switch(Self::EXTENDED_FORMAT_OPTION, "extends the decimal output to be a table instead of a float64", Some('x'))
            .switch(Self::RATIONAL_OPTION, "uses BigRational instead of BigDecimal. When used with `-x` produces the Ratio in the `text` field instead of the decimal value", Some('r'))
            .filter()
        ]
    }

    fn run(
        &mut self,
        name: &str,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        assert_eq!(name, "from parquet");
        match input {
            Value::Binary { val, span } => {
                let opts = FromParquetOpts {
                    extended_decimal: call.has_flag(Self::EXTENDED_FORMAT_OPTION),
                    rational: call.has_flag(Self::RATIONAL_OPTION),
                };
                Ok(crate::from_parquet::from_parquet_bytes(
                    val.clone(),
                    span.clone(),
                    &opts,
                ))
            }
            v => {
                return Err(LabeledError {
                    label: "Expected binary from pipeline".into(),
                    msg: format!("requires binary input, got {}", v.get_type()),
                    span: Some(call.head),
                });
            }
        }
    }
}

fn main() {
    serve_plugin(&mut FromParquet::new(), JsonSerializer);
}
