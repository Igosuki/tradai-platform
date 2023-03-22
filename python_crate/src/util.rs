use pyo3::{PyResult, Python};

pub(crate) fn register_tradai_module(py: Python) -> PyResult<()> {
    /// Modules can only be initialized once in pyo3
    let sysmod = py.import("sys")?.getattr("modules")?;
    if sysmod.get_item("tradai").is_err() {
        let m = wrap_pymodule!(crate::tradai)(py);
        sysmod.set_item("tradai", m)
    } else {
        Ok(())
    }
}
