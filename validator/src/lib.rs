use pyo3::prelude::*;
use regex::Regex;
use std::sync::OnceLock;

static USER_ID_RE: OnceLock<Regex> = OnceLock::new();

fn user_id_regex() -> &'static Regex {
    USER_ID_RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap())
}

const VALID_EVENT_TYPES: &[&str] = &[
    "click", "page_view", "purchase", "login", "logout", "search",
];

#[derive(serde::Deserialize)]
struct EventData {
    id: String,
    user_id: String,
    session_id: String,
    event_type: String,
    page: String,
    duration_ms: f64,
}

#[derive(serde::Serialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
}

fn validate(event: &EventData) -> ValidationResult {
    let mut errors = Vec::new();

    // 1. id: непустой, длина <= 36.
    if event.id.is_empty() {
        errors.push("id: must not be empty".to_string());
    } else if event.id.len() > 36 {
        errors.push(format!("id: length {} exceeds 36 chars", event.id.len()));
    }

    // 2. user_id: непустой, соответствует ^[a-zA-Z0-9_-]+$.
    if event.user_id.is_empty() {
        errors.push("user_id: must not be empty".to_string());
    } else if !user_id_regex().is_match(&event.user_id) {
        errors.push("user_id: contains invalid characters (allowed: a-z A-Z 0-9 _ -)".to_string());
    }

    // 3. session_id: непустой, длина <= 36.
    if event.session_id.is_empty() {
        errors.push("session_id: must not be empty".to_string());
    } else if event.session_id.len() > 36 {
        errors.push(format!(
            "session_id: length {} exceeds 36 chars",
            event.session_id.len()
        ));
    }

    // 4. event_type: один из допустимых значений.
    if !VALID_EVENT_TYPES.contains(&event.event_type.as_str()) {
        errors.push(format!(
            "event_type: '{}' is not valid (allowed: {})",
            event.event_type,
            VALID_EVENT_TYPES.join(", ")
        ));
    }

    // 5. page: начинается с "/".
    if !event.page.starts_with('/') {
        errors.push(format!("page: '{}' must start with '/'", event.page));
    }

    // 6. duration_ms: [0.0, 300000.0].
    if event.duration_ms < 0.0 {
        errors.push(format!(
            "duration_ms: {:.2} must be >= 0",
            event.duration_ms
        ));
    } else if event.duration_ms > 300_000.0 {
        errors.push(format!(
            "duration_ms: {:.2} exceeds max 300000 ms (5 minutes)",
            event.duration_ms
        ));
    }

    ValidationResult {
        is_valid: errors.is_empty(),
        errors,
    }
}

/// Валидирует одно событие, переданное в виде JSON-строки.
/// Возвращает JSON-сериализованный ValidationResult.
#[pyfunction]
fn validate_event(event_json: &str) -> PyResult<String> {
    let result = match serde_json::from_str::<EventData>(event_json) {
        Ok(event) => validate(&event),
        Err(e) => ValidationResult {
            is_valid: false,
            errors: vec![format!("JSON parse error: {e}")],
        },
    };
    serde_json::to_string(&result)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

/// Валидирует батч событий, переданных как JSON-массив.
/// Возвращает JSON-массив ValidationResult.
#[pyfunction]
fn validate_batch(events_json: &str) -> PyResult<String> {
    let events: Vec<serde_json::Value> = match serde_json::from_str(events_json) {
        Ok(v) => v,
        Err(e) => {
            let result = vec![ValidationResult {
                is_valid: false,
                errors: vec![format!("JSON parse error: {e}")],
            }];
            return serde_json::to_string(&result)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()));
        }
    };

    let results: Vec<ValidationResult> = events
        .iter()
        .map(|v| match serde_json::from_value::<EventData>(v.clone()) {
            Ok(event) => validate(&event),
            Err(e) => ValidationResult {
                is_valid: false,
                errors: vec![format!("JSON parse error: {e}")],
            },
        })
        .collect();

    serde_json::to_string(&results)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

#[pymodule]
fn event_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_event, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    Ok(())
}
