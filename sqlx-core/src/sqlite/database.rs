use crate::database::{Database, HasArguments, HasStatement, HasStatementCache, HasValueRef};
use crate::sqlite::{
    SqliteArgumentValue, SqliteArguments, SqliteColumn, SqliteConnection, SqliteDone, SqliteRow,
    SqliteStatement, SqliteTransactionManager, SqliteTypeInfo, SqliteValue, SqliteValueRef,
};

/// Sqlite database driver.
#[derive(Debug)]
pub struct Sqlite;

impl Database for Sqlite {
    type Connection = SqliteConnection;

    type TransactionManager = SqliteTransactionManager;

    type Row = SqliteRow;

    type Done = SqliteDone;

    type Column = SqliteColumn;

    type TypeInfo = SqliteTypeInfo;

    type Value = SqliteValue;
}

impl<'r> HasValueRef<'r> for Sqlite {
    type Database = Sqlite;

    type ValueRef = SqliteValueRef<'r>;
}

impl<'a> HasArguments<'a> for Sqlite {
    type Database = Sqlite;

    type Arguments = SqliteArguments<'a>;

    type ArgumentBuffer = Vec<SqliteArgumentValue<'a>>;
}

impl<'q, 'a> HasStatement<'q, 'a> for Sqlite {
    type Database = Sqlite;

    type Statement = SqliteStatement<'q>;
}

impl HasStatementCache for Sqlite {}
