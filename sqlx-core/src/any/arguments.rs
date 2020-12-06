use crate::any::Any;
use crate::arguments::Arguments;
use crate::encode::Encode;
use crate::types::Type;

#[derive(Default)]
pub struct AnyArguments<'a> {
    values: Vec<Box<dyn Encode<'a, Any> + Send + 'a>>,
}

impl<'a> Arguments<'a> for AnyArguments<'a> {
    type Database = Any;

    fn reserve(&mut self, additional: usize, _size: usize) {
        self.values.reserve(additional);
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'a + Send + Encode<'a, Self::Database> + Type<Self::Database>,
    {
        self.values.push(Box::new(value));
    }
}

pub struct AnyArgumentBuffer<'a>(pub(crate) AnyArgumentBufferKind<'a>);

pub(crate) enum AnyArgumentBufferKind<'a> {
    #[cfg(feature = "postgres")]
    Postgres(
        crate::postgres::PgArguments,
        std::marker::PhantomData<&'a ()>,
    ),

    #[cfg(feature = "mysql")]
    MySql(
        crate::mysql::MySqlArguments,
        std::marker::PhantomData<&'a ()>,
    ),

    #[cfg(feature = "sqlite")]
    Sqlite(crate::sqlite::SqliteArguments<'a>),

    #[cfg(feature = "mssql")]
    Mssql(
        crate::mssql::MssqlArguments,
        std::marker::PhantomData<&'a ()>,
    ),
}

// control flow inferred type bounds would be fun
// the compiler should know the branch is totally unreachable

#[cfg(feature = "sqlite")]
#[allow(irrefutable_let_patterns)]
impl<'a> From<AnyArguments<'a>> for crate::sqlite::SqliteArguments<'a> {
    fn from(args: AnyArguments<'a>) -> Self {
        let mut buf = AnyArgumentBuffer(AnyArgumentBufferKind::Sqlite(Default::default()));

        for value in args.values {
            let _ = value.encode_by_ref(&mut buf);
        }

        if let AnyArgumentBufferKind::Sqlite(args) = buf.0 {
            args
        } else {
            unreachable!()
        }
    }
}

#[cfg(feature = "mysql")]
#[allow(irrefutable_let_patterns)]
impl<'a> From<AnyArguments<'a>> for crate::mysql::MySqlArguments {
    fn from(args: AnyArguments<'a>) -> Self {
        let mut buf = AnyArgumentBuffer(AnyArgumentBufferKind::MySql(
            Default::default(),
            std::marker::PhantomData,
        ));

        for value in args.values {
            let _ = value.encode_by_ref(&mut buf);
        }

        if let AnyArgumentBufferKind::MySql(args, _) = buf.0 {
            args
        } else {
            unreachable!()
        }
    }
}

#[cfg(feature = "mssql")]
#[allow(irrefutable_let_patterns)]
impl<'a> From<AnyArguments<'a>> for crate::mssql::MssqlArguments {
    fn from(args: AnyArguments<'a>) -> Self {
        let mut buf = AnyArgumentBuffer(AnyArgumentBufferKind::Mssql(
            Default::default(),
            std::marker::PhantomData,
        ));

        for value in args.values {
            let _ = value.encode_by_ref(&mut buf);
        }

        if let AnyArgumentBufferKind::Mssql(args, _) = buf.0 {
            args
        } else {
            unreachable!()
        }
    }
}

#[cfg(feature = "postgres")]
#[allow(irrefutable_let_patterns)]
impl<'a> From<AnyArguments<'a>> for crate::postgres::PgArguments {
    fn from(args: AnyArguments<'a>) -> Self {
        let mut buf = AnyArgumentBuffer(AnyArgumentBufferKind::Postgres(
            Default::default(),
            std::marker::PhantomData,
        ));

        for value in args.values {
            let _ = value.encode_by_ref(&mut buf);
        }

        if let AnyArgumentBufferKind::Postgres(args, _) = buf.0 {
            args
        } else {
            unreachable!()
        }
    }
}
