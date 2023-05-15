use ::uuid::Uuid as UuidStd;
use pyo3::prelude::*;

#[pyclass(freelist = 1000)]
#[derive(Clone)]
pub(crate) struct Uuid {
    pub(crate) handle: UuidStd,
}

#[pyfunction(name = "uuid4")]
fn uuid4() -> Uuid {
    Uuid {
        handle: UuidStd::new_v4(),
    }
}

#[pymodule]
pub(crate) fn uuid(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Uuid>()?;
    m.add_function(wrap_pyfunction!(uuid4, m)?)?;
    Ok(())
}
