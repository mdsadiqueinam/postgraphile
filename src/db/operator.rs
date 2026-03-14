#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    NotEqual,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Op {
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
