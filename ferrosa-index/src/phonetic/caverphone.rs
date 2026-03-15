//! Caverphone 2.0 encoding algorithm.
//!
//! Caverphone was designed by David Hood for matching New Zealand English
//! names, particularly for electoral roll matching. Caverphone 2.0 produces
//! a 10-character code.
//!
//! Reference: <https://en.wikipedia.org/wiki/Caverphone>

use super::PhoneticEncoder;

/// Caverphone 2.0 encoder.
pub struct CaverphoneEncoder;

impl PhoneticEncoder for CaverphoneEncoder {
    fn encode(&self, input: &str) -> String {
        if input.is_empty() {
            return String::new();
        }

        // Step 1: Convert to lowercase, remove non-alpha
        let mut word: String = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_lowercase())
            .collect();

        if word.is_empty() {
            return String::new();
        }

        // Step 2: Remove trailing 'e'
        if word.ends_with('e') {
            word.pop();
            if word.is_empty() {
                return "1111111111".to_string();
            }
        }

        // Step 3: Handle initial replacements for -ough words
        if word.starts_with("cough") {
            word = format!("cou2f{}", &word[5..]);
        }
        if word.starts_with("rough") {
            word = format!("rou2f{}", &word[5..]);
        }
        if word.starts_with("tough") {
            word = format!("tou2f{}", &word[5..]);
        }
        if word.starts_with("enough") {
            word = format!("enou2f{}", &word[6..]);
        }
        // Handle words starting with "tro" + "ugh"
        {
            let prefix = "trough";
            if word.starts_with(prefix) {
                word = format!("trou2f{}", &word[6..]);
            }
        }

        // Replace 'gn' at start with '2n'
        if word.starts_with("gn") {
            word = format!("2n{}", &word[2..]);
        }

        // Replace 'mb' at end with 'm2'
        if word.ends_with("mb") {
            let prefix = &word[..word.len() - 2];
            word = format!("{prefix}m2");
        }

        // Apply global replacements in order
        let replacements: &[(&str, &str)] = &[
            ("cq", "2q"),
            ("ci", "si"),
            ("ce", "se"),
            ("cy", "sy"),
            ("tch", "2ch"),
            ("c", "k"),
            ("q", "k"),
            ("x", "k"),
            ("v", "f"),
            ("dg", "2g"),
            ("tio", "sio"),
            ("tia", "sia"),
            ("d", "t"),
            ("ph", "fh"),
            ("b", "p"),
            ("sh", "s2"),
            ("z", "s"),
        ];

        for &(from, to) in replacements {
            word = word.replace(from, to);
        }

        // Replace initial vowel with 'A'
        let first = word.chars().next().unwrap();
        if "aeiou".contains(first) {
            word = format!("A{}", &word[1..]);
        }

        // Remove all remaining vowels
        word = word.chars().filter(|&c| !"aeiou".contains(c)).collect();

        // Remove all instances of '2' and '3'
        word = word.chars().filter(|&c| c != '2' && c != '3').collect();

        // Collapse consecutive duplicate characters
        let mut collapsed = String::new();
        let mut last = '\0';
        for c in word.chars() {
            if c != last {
                collapsed.push(c);
            }
            last = c;
        }
        word = collapsed;

        // Pad or truncate to 10 characters
        while word.len() < 10 {
            word.push('1');
        }
        word.truncate(10);

        word.to_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caverphone_empty() {
        let enc = CaverphoneEncoder;
        assert_eq!(enc.encode(""), "");
    }

    #[test]
    fn caverphone_basic() {
        let enc = CaverphoneEncoder;
        let code = enc.encode("Thompson");
        assert!(!code.is_empty());
        assert_eq!(code.len(), 10);
    }

    #[test]
    fn caverphone_similar_names() {
        let enc = CaverphoneEncoder;
        assert_eq!(enc.encode("Lee"), enc.encode("Lea"));
    }

    #[test]
    fn caverphone_ten_chars() {
        let enc = CaverphoneEncoder;
        assert_eq!(enc.encode("A").len(), 10);
        assert_eq!(enc.encode("Smith").len(), 10);
        assert_eq!(enc.encode("Abrahamson").len(), 10);
    }

    #[test]
    fn caverphone_case_insensitive() {
        let enc = CaverphoneEncoder;
        assert_eq!(enc.encode("SMITH"), enc.encode("smith"));
    }

    #[test]
    fn caverphone_non_alpha_ignored() {
        let enc = CaverphoneEncoder;
        assert_eq!(enc.encode("O'Brien"), enc.encode("OBrien"));
    }
}
