//! Provides [`Encode`](trait.Encode.html) for encoding values for the database.

use std::mem;

use crate::database::{Database, HasArguments};

/// The return type of [Encode::encode].
pub enum IsNull {
    /// The value is null; no data was written.
    Yes,

    /// The value is not null.
    ///
    /// This does not mean that data was written.
    No,
}

/// Encode a single value to be sent to the database.
pub trait Encode<'a, DB: Database> {
    /// Writes the value of `self` into `buf` in the expected format for the database.
    #[must_use]
    fn encode(self, buf: &mut <DB as HasArguments<'a>>::ArgumentBuffer) -> IsNull
    where
        Self: Sized,
    {
        self.encode_by_ref(buf)
    }

    /// Writes the value of `self` into `buf` without moving `self`.
    ///
    /// Where possible, make use of `encode` instead as it can take advantage of re-using
    /// memory.
    #[must_use]
    fn encode_by_ref(&self, buf: &mut <DB as HasArguments<'a>>::ArgumentBuffer) -> IsNull;

    fn produces(&self) -> Option<DB::TypeInfo> {
        // `produces` is inherently a hook to allow database drivers to produce value-dependent
        // type information; if the driver doesn't need this, it can leave this as `None`
        None
    }

    #[inline]
    fn size_hint(&self) -> usize {
        mem::size_of_val(self)
    }
}

impl<'a, T, DB: Database> Encode<'a, DB> for &'_ T
where
    T: Encode<'a, DB>,
{
    #[inline]
    fn encode(self, buf: &mut <DB as HasArguments<'a>>::ArgumentBuffer) -> IsNull {
        <T as Encode<DB>>::encode_by_ref(self, buf)
    }

    #[inline]
    fn encode_by_ref(&self, buf: &mut <DB as HasArguments<'a>>::ArgumentBuffer) -> IsNull {
        <&T as Encode<DB>>::encode(self, buf)
    }

    #[inline]
    fn produces(&self) -> Option<DB::TypeInfo> {
        (**self).produces()
    }

    #[inline]
    fn size_hint(&self) -> usize {
        (**self).size_hint()
    }
}

#[allow(unused_macros)]
macro_rules! impl_encode_for_option {
    ($DB:ident) => {
        impl<'a, T> crate::encode::Encode<'a, $DB> for Option<T>
        where
            T: crate::encode::Encode<'a, $DB> + crate::types::Type<$DB> + 'a,
        {
            #[inline]
            fn produces(&self) -> Option<<$DB as crate::database::Database>::TypeInfo> {
                if let Some(v) = self {
                    v.produces()
                } else {
                    T::type_info().into()
                }
            }

            #[inline]
            fn encode(
                self,
                buf: &mut <$DB as crate::database::HasArguments<'a>>::ArgumentBuffer,
            ) -> crate::encode::IsNull {
                if let Some(v) = self {
                    v.encode(buf)
                } else {
                    crate::encode::IsNull::Yes
                }
            }

            #[inline]
            fn encode_by_ref(
                &self,
                buf: &mut <$DB as crate::database::HasArguments<'a>>::ArgumentBuffer,
            ) -> crate::encode::IsNull {
                if let Some(v) = self {
                    v.encode_by_ref(buf)
                } else {
                    crate::encode::IsNull::Yes
                }
            }

            #[inline]
            fn size_hint(&self) -> usize {
                self.as_ref().map_or(0, crate::encode::Encode::size_hint)
            }
        }
    };
}
