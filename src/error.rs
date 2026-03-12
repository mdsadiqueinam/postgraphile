/// Creates an [`async_graphql::Error`] from a displayable message.
#[inline]
pub(crate) fn gql_err(msg: impl std::fmt::Display) -> async_graphql::Error {
    async_graphql::Error::new(msg.to_string())
}
