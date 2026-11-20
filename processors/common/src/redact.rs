//! Types that keep VIN and location out of logs.
//!
//! `Debug` and `Display` always print a placeholder, so `{:?}`, `{}`,
//! `dbg!` and error messages cannot leak the value. Serialization to the
//! output sink is unchanged; use [`Vin::expose`] only for output records,
//! never for logging.

use std::fmt;

use serde::{Deserialize, Serialize};

const REDACTED: &str = "[REDACTED]";

/// Wraps any value so that formatting it prints `[REDACTED]`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Returns the wrapped value. Do not pass the result to a logger.
    pub fn expose(&self) -> &T {
        &self.0
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> fmt::Display for Redacted<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// Vehicle identification number. Never printed in clear text.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Vin(Redacted<String>);

impl Vin {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Redacted::new(value.into()))
    }

    /// Returns the VIN for output records. Do not pass the result to a logger.
    pub fn expose(&self) -> &str {
        self.0.expose()
    }

    pub fn is_empty(&self) -> bool {
        self.0.expose().is_empty()
    }
}

impl fmt::Debug for Vin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Vin({REDACTED})")
    }
}

impl fmt::Display for Vin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// GPS position of the vehicle. Never printed in clear text. A coordinate
/// is `None` when the input omits it or it is not a number.
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

impl fmt::Debug for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Location({REDACTED})")
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIN: &str = "TESTVIN0000000001";

    #[test]
    fn vin_is_redacted_in_debug_and_display() {
        let vin = Vin::new(VIN);
        assert!(!format!("{vin:?}").contains(VIN));
        assert!(!format!("{vin}").contains(VIN));
        assert!(!format!("{:?}", Some(&vin)).contains(VIN));
    }

    #[test]
    fn location_is_redacted_in_debug_and_display() {
        let loc = Location {
            lat: Some(48.1351),
            lon: Some(11.582),
        };
        for text in [format!("{loc:?}"), format!("{loc}")] {
            assert!(!text.contains("48.1351"));
            assert!(!text.contains("11.582"));
        }
    }

    #[test]
    fn redacted_hides_any_value() {
        let secret = Redacted::new("Main Street 1");
        assert_eq!(format!("{secret:?}"), REDACTED);
        assert_eq!(format!("{secret}"), REDACTED);
        assert!(
            *secret.expose() == "Main Street 1",
            "expose() changed the value"
        );
    }

    #[test]
    fn vin_serializes_in_clear_for_output() {
        let json = serde_json::to_string(&Vin::new(VIN)).unwrap();
        assert!(
            json == format!("\"{VIN}\""),
            "VIN not serialized as a plain string"
        );
        let back: Vin = serde_json::from_str(&json).unwrap();
        assert!(back.expose() == VIN, "VIN changed in a serde round trip");
    }
}
