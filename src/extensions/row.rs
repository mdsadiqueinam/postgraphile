use serde_json::{Map, Value};
use tokio_postgres::{Row, types::Type};

pub trait JsonExt {
    fn to_json(&self) -> Value;
}

pub trait JsonListExt {
    fn to_json_list(&self) -> Vec<Value>;
}

impl JsonExt for Row {
    fn to_json(&self) -> Value {
        let mut map = Map::new();

        for (i, col) in self.columns().iter().enumerate() {
            let name = col.name().to_string();

            let value = match *col.type_() {
                Type::BOOL => self
                    .try_get::<_, bool>(i)
                    .map(Value::Bool)
                    .unwrap_or(Value::Null),

                Type::INT2 => self
                    .try_get::<usize, i16>(i)
                    .map(|v| Value::Number(v.into()))
                    .unwrap_or(Value::Null),

                Type::INT4 => self
                    .try_get::<usize, i32>(i)
                    .map(|v| Value::Number(v.into()))
                    .unwrap_or(Value::Null),

                Type::INT8 => self
                    .try_get::<usize, i64>(i)
                    .map(|v| Value::Number(v.into()))
                    .unwrap_or(Value::Null),

                Type::FLOAT4 => self
                    .try_get::<usize, f32>(i)
                    .ok()
                    .and_then(|v| serde_json::Number::from_f64(v as f64))
                    .map(Value::Number)
                    .unwrap_or(Value::Null),

                Type::FLOAT8 => self
                    .try_get::<usize, f64>(i)
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),

                Type::TEXT | Type::VARCHAR | Type::CHAR | Type::CHAR_ARRAY => self
                    .try_get::<usize, String>(i)
                    .map(Value::String)
                    .unwrap_or(Value::Null),

                Type::JSON | Type::JSONB => self.try_get::<usize, Value>(i).unwrap_or(Value::Null),

                _ => self
                    .try_get::<usize, String>(i)
                    .map(Value::String)
                    .unwrap_or(Value::Null), // fallback to string
            };

            map.insert(name, value);
        }

        Value::Object(map)
    }
}

impl JsonExt for Vec<Row> {
    fn to_json(&self) -> Value {
        let values = self.to_json_list();
        Value::Array(values)
    }
}

impl JsonListExt for Vec<Row> {
    fn to_json_list(&self) -> Vec<Value> {
        self.iter().map(|row| row.to_json()).collect::<Vec<Value>>()
    }
}
