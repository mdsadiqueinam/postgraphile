use async_graphql::dynamic::{Enum, EnumItem, InputObject, InputValue, TypeRef};
use tokio_postgres::types::Type;

use crate::table::Table;
use crate::utils::inflection::to_pascal_case;

use super::type_mapping::condition_type_ref;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    NotEqual,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl FilterOp {
    pub fn from_key(key: &str) -> Option<Self> {
        match key {
            "equal" => Some(Self::Eq),
            "notEqual" => Some(Self::NotEqual),
            "in" => Some(Self::In),
            "greaterThan" => Some(Self::Gt),
            "greaterThanEqual" => Some(Self::Gte),
            "lessThan" => Some(Self::Lt),
            "lessThanEqual" => Some(Self::Lte),
            _ => None,
        }
    }

    pub fn sql_operator(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEqual => "<>",
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::In => unreachable!("IN is not a simple binary operator"),
        }
    }

    pub fn is_range(self) -> bool {
        matches!(self, Self::Gt | Self::Gte | Self::Lt | Self::Lte)
    }
}

pub fn supports_range(column_type: &Type) -> bool {
    matches!(
        *column_type,
        Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::DATE
            | Type::TIME
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
    )
}

/// Builds per-column `{TypeName}{Column}Filter` input objects referenced by
/// `{TypeName}Condition`. Exported so callers can register them with the schema.
pub fn make_condition_filter_types(table: &Table) -> Vec<InputObject> {
    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .filter_map(|col| {
            condition_type_ref(col).map(|tr| {
                let scalar_name = tr.to_string();
                let filter_name =
                    format!("{}{}Filter", table.type_name(), to_pascal_case(col.name()));

                // example generated input object for a "email" column of type String:
                // input UserEmailFilter {
                //   equal: String
                // }
                let mut input = InputObject::new(filter_name)
                    .field(InputValue::new("equal", tr.clone()))
                    .field(InputValue::new("notEqual", tr.clone()))
                    .field(InputValue::new("in", TypeRef::named_list(scalar_name)));

                if supports_range(col._type()) {
                    input = input
                        .field(InputValue::new("greaterThan", tr.clone()))
                        .field(InputValue::new("greaterThanEqual", tr.clone()))
                        .field(InputValue::new("lessThan", tr.clone()))
                        .field(InputValue::new("lessThanEqual", tr));
                }

                input
            })
        })
        .collect()
}

/// Builds the `{TypeName}Condition` input object (per-column operator filters).
/// Exported so callers can register it with the schema separately.
pub fn make_condition_type(table: &Table) -> InputObject {
    let name = format!("{}Condition", table.type_name());

    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .fold(InputObject::new(name), |obj, col| {
            if condition_type_ref(col).is_some() {
                let filter_name =
                    format!("{}{}Filter", table.type_name(), to_pascal_case(col.name()));
                obj.field(InputValue::new(
                    col.name().as_str(),
                    TypeRef::named(filter_name),
                ))
            } else {
                obj
            }
        })
}

/// Builds the `{TypeName}OrderBy` enum (COLUMN_ASC / COLUMN_DESC per column).
/// Exported so callers can register it with the schema separately.
pub fn make_order_by_enum(table: &Table) -> Enum {
    let name = format!("{}OrderBy", table.type_name());
    table
        .columns()
        .iter()
        .filter(|c| !c.omit_read())
        .flat_map(|c| {
            let upper = c.name().to_uppercase();
            [
                EnumItem::new(format!("{}_ASC", upper)),
                EnumItem::new(format!("{}_DESC", upper)),
            ]
        })
        .fold(Enum::new(name), |e, item| e.item(item))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;
    use tokio_postgres::types::Type;

    #[test]
    fn test_condition_type_name() {
        let table = Table::new_for_test("blog_posts", vec![]);
        assert_eq!(make_condition_type(&table).type_name(), "BlogPostCondition");
    }

    #[test]
    fn test_condition_type_name_users() {
        let table = Table::new_for_test("users", vec![]);
        assert_eq!(make_condition_type(&table).type_name(), "UserCondition");
    }

    #[test]
    fn test_order_by_enum_name() {
        let table = Table::new_for_test("blog_posts", vec![]);
        assert_eq!(make_order_by_enum(&table).type_name(), "BlogPostOrderBy");
    }

    #[test]
    fn test_order_by_enum_name_users() {
        let table = Table::new_for_test("users", vec![]);
        assert_eq!(make_order_by_enum(&table).type_name(), "UserOrderBy");
    }

    #[test]
    fn test_filter_op_from_key_not_equal() {
        assert_eq!(FilterOp::from_key("notEqual"), Some(FilterOp::NotEqual));
    }

    #[test]
    fn test_filter_op_from_key_range() {
        assert_eq!(FilterOp::from_key("greaterThanEqual"), Some(FilterOp::Gte));
        assert_eq!(FilterOp::from_key("lessThan"), Some(FilterOp::Lt));
    }

    #[test]
    fn test_filter_op_from_key_default_eq() {
        assert_eq!(FilterOp::from_key("equal"), Some(FilterOp::Eq));
    }

    #[test]
    fn test_filter_op_from_key_unknown() {
        assert_eq!(FilterOp::from_key("between"), None);
    }

    #[test]
    fn test_filter_op_sql_operator() {
        assert_eq!(FilterOp::Eq.sql_operator(), "=");
        assert_eq!(FilterOp::NotEqual.sql_operator(), "<>");
        assert_eq!(FilterOp::Gt.sql_operator(), ">");
        assert_eq!(FilterOp::Gte.sql_operator(), ">=");
        assert_eq!(FilterOp::Lt.sql_operator(), "<");
        assert_eq!(FilterOp::Lte.sql_operator(), "<=");
    }

    #[test]
    fn test_filter_op_is_range() {
        assert!(!FilterOp::Eq.is_range());
        assert!(!FilterOp::NotEqual.is_range());
        assert!(!FilterOp::In.is_range());
        assert!(FilterOp::Gt.is_range());
        assert!(FilterOp::Gte.is_range());
        assert!(FilterOp::Lt.is_range());
        assert!(FilterOp::Lte.is_range());
    }

    #[test]
    fn test_supports_range_for_numeric() {
        assert!(supports_range(&Type::INT2));
        assert!(supports_range(&Type::INT4));
        assert!(supports_range(&Type::INT8));
        assert!(supports_range(&Type::FLOAT4));
        assert!(supports_range(&Type::FLOAT8));
        assert!(supports_range(&Type::NUMERIC));
    }

    #[test]
    fn test_supports_range_for_datetime() {
        assert!(supports_range(&Type::DATE));
        assert!(supports_range(&Type::TIME));
        assert!(supports_range(&Type::TIMESTAMP));
        assert!(supports_range(&Type::TIMESTAMPTZ));
        // TIMETZ is excluded — no simple ToSql mapping available
        assert!(!supports_range(&Type::TIMETZ));
    }

    #[test]
    fn test_supports_range_for_non_numeric() {
        assert!(!supports_range(&Type::TEXT));
        assert!(!supports_range(&Type::BOOL));
        assert!(!supports_range(&Type::JSON));
    }
}
