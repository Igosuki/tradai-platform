use chrono::{DateTime, Utc};

pub mod orderbook;
pub mod trades;

pub(crate) fn event_ms_where_clause(
    col: &str,
    upper_dt: Option<DateTime<Utc>>,
    lower_dt: Option<DateTime<Utc>>,
) -> Vec<String> {
    let mut clauses = vec![];
    if let Some(upper) = upper_dt {
        clauses.push(format!("{} <= {}", col, upper.timestamp_millis()));
    }
    if let Some(lower) = lower_dt {
        clauses.push(format!("{} >= {}", col, lower.timestamp_millis()));
    }
    clauses
}

pub(crate) fn join_where_clause<T: AsRef<[String]>>(clauses: T) -> String {
    if !clauses.as_ref().is_empty() {
        format!("where {}", clauses.as_ref().join(" and "))
    } else {
        String::new()
    }
}

pub(crate) fn in_clause(col: &str, clauses: Vec<String>) -> String {
    if !clauses.is_empty() {
        format!(
            "{} IN ({})",
            col,
            clauses
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<String>>()
                .join(", ")
        )
    } else {
        String::new()
    }
}
