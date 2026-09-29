//! Fuzz `JsonbRef::validate` (T-104, FM-05, JB-T1): no panic on any bytes, and an
//! accepted cell re-encodes from the reader to exactly the same bytes, so
//! `validate` accepts a string only if it is canonical.
#![no_main]

use ferrosa_jsonb::{
    ArrayIter, JsonbBuilder, JsonbError, JsonbRef, Limits, LimitsConfig, ObjectIter, ValueKind,
    ValueRef,
};
use libfuzzer_sys::fuzz_target;

enum Walk<'a> {
    Obj(ObjectIter<'a>),
    Arr(ArrayIter<'a>),
}

fn open<'a>(b: &mut JsonbBuilder, v: ValueRef<'a>) -> Result<Option<Walk<'a>>, JsonbError> {
    match v.kind()? {
        ValueKind::Object => {
            b.begin_object()?;
            Ok(Some(Walk::Obj(v.as_object()?.iter())))
        }
        ValueKind::Array => {
            b.begin_array()?;
            Ok(Some(Walk::Arr(v.as_array()?.iter())))
        }
        ValueKind::Null => b.null().map(|()| None),
        ValueKind::Bool => b.boolean(v.as_bool()?).map(|()| None),
        ValueKind::Number => b.number(v.as_number()?).map(|()| None),
        ValueKind::String => b.string(v.as_str()?).map(|()| None),
    }
}

fn rebuild(r: &JsonbRef<'_>, limits: Limits) -> Result<Vec<u8>, JsonbError> {
    let mut b = JsonbBuilder::new(limits);
    let mut stack: Vec<Walk<'_>> = Vec::new();
    stack.extend(open(&mut b, r.root())?);
    while let Some(top) = stack.last_mut() {
        let next = match top {
            Walk::Obj(it) => match it.next() {
                Some(item) => {
                    let (key, v) = item?;
                    b.key(key)?;
                    Some(v)
                }
                None => None,
            },
            Walk::Arr(it) => it.next().transpose()?,
        };
        match next {
            Some(v) => stack.extend(open(&mut b, v)?),
            None => match stack.pop() {
                Some(Walk::Obj(_)) => b.end_object()?,
                Some(Walk::Arr(_)) => b.end_array()?,
                None => {}
            },
        }
    }
    Ok(b.finish()?.bytes)
}

fuzz_target!(|data: &[u8]| {
    let Ok(r) = JsonbRef::validate(data) else {
        return;
    };
    let limits = Limits::from_config(&LimitsConfig::default(), 512 * 1024 * 1024)
        .expect("default limits are valid");
    // A cell above the 10 MiB tunable was ingested under a larger limit; the
    // re-encode check only applies inside the default limits.
    if data.len() <= 10 * 1024 * 1024 {
        assert_eq!(rebuild(&r, limits).expect("accepted cell rebuilds"), data);
    }
});
