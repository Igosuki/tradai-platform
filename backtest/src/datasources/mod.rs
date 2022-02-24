use chrono::{DateTime, Utc};

pub mod orderbook;
pub mod trades;

pub(crate) fn event_ms_where_clause(
    col: &str,
    upper_dt: Option<DateTime<Utc>>,
    lower_dt: Option<DateTime<Utc>>,
) -> String {
    let mut clauses = vec![];
    if let Some(upper) = upper_dt {
        clauses.push(format!("{} <= {}", col, upper.timestamp_millis()));
    }
    if let Some(lower) = lower_dt {
        clauses.push(format!("{} >= {}", col, lower.timestamp_millis()));
    }
    if !clauses.is_empty() {
        format!("where {}", clauses.join(" and "))
    } else {
        String::new()
    }
}
