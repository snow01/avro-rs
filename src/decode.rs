use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::mem::transmute;

use crate::schema::{Schema, UnionRecordSchema};
use crate::types::{LruValue, Value};
use crate::util::{safe_len, zag_i32, zag_i64, DecodeError};
use failure::Error;

#[inline]
fn decode_counter<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(|v| Value::Counter(v, None))
}

#[inline]
fn decode_date<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(|v| Value::Date(v, None))
}

#[inline]
fn decode_long<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i64(reader).map(|v| Value::Long(v, None))
}

#[inline]
fn decode_int<R: Read>(reader: &mut R) -> Result<Value, Error> {
    zag_i32(reader).map(|v| Value::Int(v, None))
}

#[inline]
fn decode_len<R: Read>(reader: &mut R) -> Result<usize, Error> {
    zag_i64(reader).and_then(|len| safe_len(len as usize))
}

/// Decode a `Value` from avro format given its `Schema`.
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Result<Value, Error> {
    match *schema {
        Schema::Null => Ok(Value::Null),
        Schema::Boolean => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf[..])?;

            match buf[0] {
                0u8 => Ok(Value::Boolean(false, None)),
                1u8 => Ok(Value::Boolean(true, None)),
                _ => Err(DecodeError::new("not a bool").into()),
            }
        }
        Schema::Int => decode_int(reader),
        Schema::Long => decode_long(reader),
        Schema::Float => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Float(
                unsafe { transmute::<[u8; 4], f32>(buf) },
                None,
            ))
        }
        Schema::Double => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf[..])?;
            Ok(Value::Double(
                unsafe { transmute::<[u8; 8], f64>(buf) },
                None,
            ))
        }
        Schema::Bytes => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;
            Ok(Value::Bytes(buf, None))
        }
        Schema::String => {
            let len = decode_len(reader)?;
            let mut buf = Vec::with_capacity(len);
            unsafe {
                buf.set_len(len);
            }
            reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map(|v| Value::String(v, None))
                .map_err(|_| DecodeError::new("not a valid utf-8 string").into())
        }
        Schema::Fixed { size, .. } => {
            let mut buf = vec![0u8; size as usize];
            reader.read_exact(&mut buf)?;
            Ok(Value::Fixed(size, buf, None))
        }
        Schema::Array(ref inner) => {
            let mut items = Vec::new();

            loop {
                let len = decode_len(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    items.push(decode(inner, reader)?);
                }
            }

            Ok(Value::Array(items, None))
        }
        Schema::Map(ref inner) => {
            let mut items = HashMap::new();

            loop {
                let len = decode_len(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key, _) = decode(&Schema::String, reader)? {
                        let value = decode(inner, reader)?;
                        items.insert(key, value);
                    } else {
                        return Err(DecodeError::new("map key is not a string").into());
                    }
                }
            }

            Ok(Value::Map(items, None))
        }
        Schema::Union(ref inner) => {
            let index = zag_i64(reader)?;
            let variants = inner.variants();
            match variants.get(index as usize) {
                Some(variant) => decode(variant, reader).map(|x| Value::Union(Box::new(x), None)),
                None => Err(DecodeError::new("Union index out of bounds").into()),
            }
        }
        Schema::Record {
            ref fields, /*, allow_partial*/
            ..
        } => {
            // Benchmarks indicate ~10% improvement using this method.
            fields
                .iter()
                .map(|field| decode(&field.schema, reader).map(|value| (field.name.clone(), value)))
                .collect::<Result<Vec<(String, Value)>, _>>()
                .map(|items| Value::Record(items, None))
        }
        Schema::Enum { ref symbols, .. } => {
            if let Value::Int(index, _) = decode_int(reader)? {
                if index >= 0 && (index as usize) <= symbols.len() {
                    let symbol = symbols[index as usize].clone();
                    Ok(Value::Enum(index, symbol, None))
                } else {
                    Err(DecodeError::new("enum symbol index out of bounds").into())
                }
            } else {
                Err(DecodeError::new("enum symbol not found").into())
            }
        }
        Schema::Date(_) => decode_date(reader),
        Schema::Set => {
            let mut items: HashSet<String> = HashSet::new();

            loop {
                let len = decode_len(reader)?;
                // arrays are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the array
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(val, _) = decode(&Schema::String, reader)? {
                        items.insert(val);
                    } else {
                        return Err(DecodeError::new("set value is not a string").into());
                    }
                }
            }

            Ok(Value::Set(items, None))
        }
        Schema::LruSet(ref lru_limit) => {
            let mut items: HashMap<String, LruValue> = HashMap::new();

            loop {
                let len = decode_len(reader)?;
                // maps are 0-terminated, 0i64 is also encoded as 0 in Avro
                // reading a length of 0 means the end of the map
                if len == 0 {
                    break;
                }

                items.reserve(len as usize);
                for _ in 0..len {
                    if let Value::String(key, _) = decode(&Schema::String, reader)? {
                        let access_time = zag_i64(reader)?;
                        let count = zag_i64(reader)?;
                        items.insert(key, LruValue::new(access_time, count));
                    } else {
                        return Err(DecodeError::new("map key is not a string").into());
                    }
                }
            }

            Ok(Value::LruSet(items, lru_limit.clone(), None))
        }
        Schema::Optional(ref inner) => {
            let index = zag_i64(reader)?;
            match index {
                0 => Ok(Value::Optional(None, None)),
                1 => decode(inner, reader).map(|x| Value::Optional(Some(Box::new(x)), None)),
                _ => Err(DecodeError::new("Optional index out of bounds").into()),
            }
        }
        Schema::Counter => decode_counter(reader),
        Schema::Max(ref inner) => decode(inner, reader).map(|x| Value::Max(Box::new(x), None)),
        Schema::UnionRecord(ref inner) => decode_union_record(reader, inner),
        Schema::Decay(ref inner, ref decay_meta) =>{
            let v = decode(inner, reader)?;
            Ok(Value::DecayRecord(Box::new(v),decay_meta.to_decay_settings()))
        },
        Schema::ValueComparator(ref inner, condition) => {
            let v = decode(inner, reader)?;
            Ok(Value::ValueComparator(Box::new(v), condition, None))
        }
    }
}

fn decode_union_record<R: Read>(reader: &mut R, inner: &UnionRecordSchema) -> Result<Value, Error> {
    let index = zag_i64(reader)?;
    let variants = inner.variants();
    match variants.get(index as usize) {
        Some(variant) => {
            if let Schema::Record { name, .. } = variant {
                let name = name.name.clone();
                decode(variant, reader).map(|x| Value::UnionRecord(Box::new(x), name, None))
            } else {
                Err(DecodeError::new("only record types are allowed inside UnionRecord").into())
            }
        }
        None => Err(DecodeError::new("UnionRecord index out of bounds").into()),
    }
}
