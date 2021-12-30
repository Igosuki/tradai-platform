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
