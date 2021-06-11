Please learn the following patterns before starting to code on this codebase. 

### Links

- https://deterministic.space/elegant-apis-in-rust.html

### Idioms

- Do not use clone every time there is a borrow checker error, see if &, Rc or Arc can be used
- Use explicit lint rules (both deny and allow) and no general rules. N.B. : lint enforcement can be added when invoking cargo.
- No `impl Deref`
- Prefer borrowed type over owned type (&str vs &String, &[T] vs &Vec<T> and &T vs &Box<T>
- Use `format!` to build strings
- Possibly use impl Drop to implement `finally` semantics
- When mutating an enum to another value of the same enum, mem::replace and mem::take can be used to copy values without cloning
- Option implements IntoIterator, can be chained with iters, and extend vectors
- Use blocks when rebinding and moving variables into closures, so that the clones and borrows are scoped with the closure they are only used for
- Use a phantomatic private field in public structs that get dereferenced with pattern matching so that fields can be added without breaking backwards compat
- Remember rustdoc examples are ran by tests
- When mutability is only needed within a certain scope of a block or function body, use rebinding or blocks to scope the mutability and return an immutable
- When heavily relying on Option<T>, you can use `impl From<T> for Option<T>` to pass T instead of Some(T) as argument
- Pass around `IntoIterator`s when only lazy iteration is necessary instead of vec operations to allow more types and also circumvent allocation
- Return `impl Trait`s when necessary, such as iterator
- Existing traits can be extended by defining a new trait in the current crate
- Use derive_builder to automatically generate builder patterns
- Implement workflows as state machines using types
- 

### Design Patterns

While Rust is not Java, there are still patterns that are of use, and even translations of classic design patterns.

- Synthetize behavior using command pattern. For instance a Foo manipulating Bars and only calling several methods describing behaviors belonging to all bars (ex: schema wrapping migrations) . This can be implemented both with traits or a struct wrapping fn pointers.
- Use newtypes to restrict functionality : priv Foo; pub struct Bar(Foo)
- Use Deref Drop and ::new to do initialization and manage resource allocation
- Prefer small crates that do one thing well.
- Break down structs into smaller structs when code gets too large

