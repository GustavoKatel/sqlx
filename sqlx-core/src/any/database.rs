use crate::any::{
    AnyArgumentBuffer, AnyArguments, AnyColumn, AnyConnection, AnyDone, AnyRow, AnyStatement,
    AnyTransactionManager, AnyTypeInfo, AnyValue, AnyValueRef,
};
use crate::database::{Database, HasArguments, HasStatement, HasStatementCache, HasValueRef};

/// Opaque database driver. Capable of being used in place of any SQLx database driver. The actual
/// driver used will be selected at runtime, from the connection uri.
#[derive(Debug)]
pub struct Any;

impl Database for Any {
    type Connection = AnyConnection;

    type TransactionManager = AnyTransactionManager;

    type Row = AnyRow;

    type Done = AnyDone;

    type Column = AnyColumn;

    type TypeInfo = AnyTypeInfo;

    type Value = AnyValue;
}

impl<'r> HasValueRef<'r> for Any {
    type Database = Any;

    type ValueRef = AnyValueRef<'r>;
}

impl<'q, 'a> HasStatement<'q, 'a> for Any {
    type Database = Any;

    type Statement = AnyStatement<'q>;
}

impl<'a> HasArguments<'a> for Any {
    type Database = Any;

    type Arguments = AnyArguments<'a>;

    type ArgumentBuffer = AnyArgumentBuffer<'a>;
}

// This _may_ be true, depending on the selected database
impl HasStatementCache for Any {}
