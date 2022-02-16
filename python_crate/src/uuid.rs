use pyo3::prelude::*;
use uuid::Uuid as UuidStd;

#[pyclass(freelist = 1000)]
#[derive(Clone)]
pub(crate) struct Uuid {
    pub(crate) handle: UuidStd,
}

#[pyfunction(name = "uuid4", module = "uuid")]
fn uuid4() -> Uuid {
    Uuid {
        handle: UuidStd::new_v4(),
    }
}

pub(crate) fn init_module(m: &PyModule) -> PyResult<()> {
    m.add_class::<Uuid>()?;
    m.add_function(wrap_pyfunction!(uuid4, m)?)?;
    Ok(())
}
