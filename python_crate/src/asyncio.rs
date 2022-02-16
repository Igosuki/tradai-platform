use pyo3::{PyObject, Python, ToPyObject};

pub(crate) fn get_event_loop() -> PyObject {
    Python::with_gil(|py| {
        let asyncio = py.import("asyncio").unwrap();
        let event_loop = asyncio.call_method0("get_event_loop").unwrap_or_else(|_| {
            let event_loop = asyncio.call_method0("new_event_loop").unwrap();
            asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
            event_loop
        });
        event_loop.to_object(py)
    })
}
