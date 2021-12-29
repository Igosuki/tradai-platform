///! Standard library extensions through traits
///! use [`ext`]
use std::any::Any;
use std::sync::Arc;

use itertools::Itertools;

pub trait MapInto<T> {
    /// Map the contained `T` into a different type.
    ///
    /// Essentially, `.map_into()` is the same as doing `.map(|val| val.into())`.
    ///
    /// **Example**
    ///
    /// ```rust
    /// use crate::ext::MapInto;
    ///
    /// let a: Option<u8> = Some(10);
    /// let b: Option<u16> = a.map_into();
    /// assert_eq!(b, Some(10u16));
    /// ```
    fn map_into(self) -> T;
}

impl<S, T, E> MapInto<Result<T, E>> for Result<S, E>
where
    S: Into<T>,
{
    #[inline]
    fn map_into(self) -> Result<T, E> { self.map(Into::into) }
}

impl<S, T> MapInto<Option<T>> for Option<S>
where
    S: Into<T>,
{
    #[inline]
    fn map_into(self) -> Option<T> { self.map(Into::into) }
}

impl<S, T> MapInto<Vec<T>> for Vec<S>
where
    S: Into<T>,
{
    #[inline]
    fn map_into(self) -> Vec<T> { self.into_iter().map_into().collect() }
}

pub trait ResultExt<T, E> {
    /// # Errors
    ///
    /// Will convert the `Err` type to the expected return type if Into exists.
    fn err_into<U>(self) -> Result<T, U>
    where
        E: Into<U>;
}

impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn err_into<U>(self) -> Result<T, U>
    where
        E: Into<U>,
    {
        self.map_err(Into::into)
    }
}

pub trait ToAny {
    fn to_any<'a>(self: Arc<Self>) -> Arc<dyn Any + 'a>
    where
        Self: 'a;
}

impl<T: Any> ToAny for T {
    fn to_any<'a>(self: Arc<Self>) -> Arc<dyn Any + 'a>
    where
        Self: 'a,
    {
        self
    }
}

pub mod prelude {
    pub use super::{MapInto, ResultExt, ToAny};
}
