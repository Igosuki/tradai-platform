#[macro_use]
extern crate python;

use pyo3::{prelude::*, wrap_pyfunction};
use python::Context;

#[pyfunction]
fn rust_print(x: i32) {
    println!("rust: x = {}", x);
}

fn main() {
    let c = Context::new();

    c.add_wrapped(wrap_pyfunction!(rust_print));

    c.run(python! {
        x = 123
        print("python: x =", x)
        rust_print(x)
    });
}
