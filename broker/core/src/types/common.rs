use string_cache::DefaultAtom as Atom;

pub type Amount = f64;
pub type Price = f64;
pub type Volume = f64;

/// A Pair is a coinnect side representation of a trading pair such as 'BTC_USDT'
pub type Pair = Atom;

/// A Symbol is the exchange side representation of a trading pair
pub type Symbol = Atom;

/// A Symbol is the coinnect side representation of an asset such as 'BTC' or 'USD'
pub type Asset = Atom;
