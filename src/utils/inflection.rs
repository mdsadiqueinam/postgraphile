use regex::Regex;
use std::sync::LazyLock;

/// Convert a string to camelCase. It can handle PascalCase, snake_case, and kebab-case.
/// Examples:
/// - "PascalCase" -> "pascalCase"
/// - "snake_case" -> "snakeCase"
/// - "kebab-case" -> "kebabCase"   
/// Note: It will also handle mixed cases like "APIResponse" -> "apiResponse"
/// - "API_Response" -> "apiResponse"
pub fn to_camel_case(text: &str) -> String {
    // Insert a space before capital letters (PascalCase → Pascal Case)
    static RE_PASCAL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([a-z0-9])([A-Z])").unwrap());

    // Replace underscores and hyphens with spaces
    static RE_SEP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[_-]+").unwrap());

    // Split with one or more space
    static RE_SPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

    let res = RE_PASCAL.replace_all(text, "$1 $2");
    let res = RE_SEP.replace_all(&res, " ");
    let res = RE_SPACE.split(&res);

    res.enumerate()
        .map(|(idx, word)| {
            if idx == 0 {
                word.to_lowercase()
            } else {
                capitalize_first(word)
            }
        })
        .collect::<Vec<String>>()
        .join("")
        .trim()
        .to_owned()
}

fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first_char) => {
            // Collect the uppercase first char into a String
            let capitalized_first: String = first_char.to_uppercase().collect();
            // Concatenate with the rest of the string
            capitalized_first + chars.as_str()
        }
    }
}

/// Convert a string to snake_case. It can handle PascalCase, camelCase, and kebab-case.
/// Examples:
/// - "PascalCase" -> "pascal_case"
/// - "camelCase" -> "camel_case"
/// - "kebab-case" -> "kebab_case"
/// Note: It will also handle mixed cases like "APIResponse" -> "api_response"
/// - "API_Response" -> "api_response"
pub fn to_snake_case(text: &str) -> String {
    // 1. Compile Regexes only once for performance
    static RE_HYPHEN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"-").unwrap());
    static RE_LOWER_UPPER: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"([a-z0-9])([A-Z])").unwrap());
    static RE_ACRONYM: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"([A-Z]+)([A-Z][a-z0-9])").unwrap());

    // 2. Apply transformations
    let res = RE_HYPHEN.replace_all(text, "_");

    // First, handle cases like "APIResponse" -> "API_Response"
    let res = RE_ACRONYM.replace_all(&res, "${1}_$2");

    // Then, handle cases like "camelCase" -> "camel_Case"
    let res = RE_LOWER_UPPER.replace_all(&res, "${1}_$2");

    res.to_lowercase()
}

pub fn singularize(text: &str) -> String {
    return pluralizer::pluralize(text, 1, false);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pascal_to_camel_case() {
        let res = to_camel_case("PascalCase");
        assert_eq!(res, "pascalCase");
    }

    #[test]
    fn test_snake_to_camel_case() {
        let res = to_camel_case("snake_case");
        assert_eq!(res, "snakeCase");
    }

    #[test]
    fn test_kebab_to_camel_case() {
        let res = to_camel_case("kebab-case");
        assert_eq!(res, "kebabCase");
    }

    #[test]
    fn test_pascal_to_snake_case() {
        let res = to_snake_case("PascalCase");
        assert_eq!(res, "pascal_case");
    }

    #[test]
    fn test_camel_to_snake_case() {
        let res = to_snake_case("camelCase");
        assert_eq!(res, "camel_case");
    }

    #[test]
    fn test_kebab_to_snake_case() {
        let res = to_snake_case("kebab-case");
        assert_eq!(res, "kebab_case");
    }

    #[test]
    fn test_singularize() {
        assert_eq!(singularize("countries"), "country");
        assert_eq!(singularize("states"), "state");
    }
}
