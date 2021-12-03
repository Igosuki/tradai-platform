pub struct Flag {
    pub short: char,
    pub name: &'static str,
    /* ... */
}

impl Flag {
    pub fn new(short: char, name: &'static str) -> Self { Flag { short, name } }
}

inventory::collect!(Flag);
