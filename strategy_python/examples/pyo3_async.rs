use pyo3::prelude::*;

#[pyo3_asyncio::tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> PyResult<()> {
    let code = r#"
async def call_me():
    return "called"
    "#;
    let fut = Python::with_gil(|py| {
        PyModule::from_code(py, code, "thecode", "thecode")?;
        let thecode = py.import("thecode")?;

        // convert asyncio.sleep into a Rust Future
        pyo3_asyncio::tokio::into_future(thecode.call_method0("call_me")?)
    })?;

    println!("sleeping for 1s");
    let py1 = fut.await?;
    let r: String = Python::with_gil(|py| py1.extract(py))?;
    eprintln!("r = {:?}", r);
    println!("done");

    Ok(())
}

// Dump
//
// fn dump1() {
//     let locals = Python::with_gil(|py| {
//         let asyncio = py.import("asyncio").unwrap();
//         let event_loop = asyncio.call_method0("get_event_loop").unwrap_or_else(|_| {
//             let event_loop = asyncio.call_method0("new_event_loop").unwrap();
//             asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
//             event_loop
//         });
//         pyo3_asyncio::TaskLocals::new(PyObject::from(event_loop).as_ref(py))
//         //pyo3_asyncio::tokio::get_current_locals(py)
//     });
//     let event_loop = Python::with_gil(|py| {
//         let asyncio = py.import("asyncio").unwrap();
//         asyncio
//             .call_method0("get_event_loop")
//             .unwrap_or_else(|_| {
//                 let event_loop = asyncio.call_method0("new_event_loop").unwrap();
//                 asyncio.call_method1("set_event_loop", (event_loop,)).unwrap();
//                 event_loop
//             })
//             .into_py(py)
//     });
// }
//
// async fn dump2() {
//     let py_fut_r = tokio::task::spawn_blocking(move || {
//         tokio::task::block_in_place(move || {
//             tokio::runtime::Handle::current().block_on(async move {
//                 pyo3_asyncio::tokio::scope_local(locals.clone(), async move {
//                     Python::with_gil(|py| {
//                         let py1 = inner.call_method1(py, "eval", (0.0,))?;
//                         let coro = py1.as_ref(py);
//                         pyo3_asyncio::tokio::into_future(coro)
//                     })?
//                     .await
//                 })
//                 .await
//             })
//         })
//     })
//     .await
//     .unwrap();
// }
