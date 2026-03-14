use super::operator::Op;
use super::scalar::SqlScalar;

/// The Public API
pub trait WhereBuilder {
    fn where_clause(&mut self, op: Op, scalar: Option<SqlScalar>) -> &mut Self;
    fn or_where_clause(&mut self, op: Op, scalar: Option<SqlScalar>) -> &mut Self;
    fn where_block<F>(&mut self, block: F) -> &mut Self
    where
        F: FnOnce(&mut Self);
    fn or_where_block<F>(&mut self, block: F) -> &mut Self
    where
        F: FnOnce(&mut Self);
}

/// This is an internal-only trait.
/// It's not public, so users of your library won't see it.
pub(super) trait WhereInternal {
    fn get_has_where(&self) -> bool;
    fn set_has_where(&mut self, has_where: bool);
    fn get_query(&self) -> &str;
    fn push_to_query(&mut self, query: String);
    fn push_param(&mut self, scalar: Option<SqlScalar>) -> usize;

    // We can move the "guesswork" logic here
    fn get_logical_sep(&mut self) -> &str {
        if !self.get_has_where() {
            self.set_has_where(true);
            " WHERE "
        } else {
            let query = self.get_query().trim();
            if query.ends_with("AND") || query.ends_with("OR") || query.ends_with('(') {
                ""
            } else {
                " AND "
            }
        }
    }

    fn push_query_with_logical_sep(&mut self, query: String) {
        let sep = self.get_logical_sep().to_string();
        self.push_to_query(format!(" {sep} {query}"));
    }
}

impl<T: WhereInternal> WhereBuilder for T {
    fn where_clause(&mut self, op: Op, scalar: Option<SqlScalar>) -> &mut Self {
        let operator_str = scalar
            .is_some()
            .then_some(op.sql_operator())
            .unwrap_or("IS");
        let param_num = self.push_param(scalar);
        self.push_query_with_logical_sep(format!(" {operator_str} ${param_num}"));
        self
    }

    fn or_where_clause(&mut self, op: Op, scalar: Option<SqlScalar>) -> &mut Self {
        if self.get_has_where() {
            self.push_to_query(" OR ".to_string());
        }
        self.where_clause(op, scalar)
    }

    fn where_block<F>(&mut self, block: F) -> &mut Self
    where
        F: FnOnce(&mut Self),
    {
        if !self.get_has_where() {
            self.set_has_where(true);
            self.push_to_query(" WHERE ".to_string());
        }
        self.push_to_query(" (".to_string());
        block(self);
        self.push_to_query(")".to_string());
        self
    }

    fn or_where_block<F>(&mut self, block: F) -> &mut Self
    where
        F: FnOnce(&mut Self),
    {
        if self.get_has_where() {
            self.push_to_query(" OR ".to_string());
        }
        self.where_block(block)
    }
}
