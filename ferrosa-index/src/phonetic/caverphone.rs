//! Caverphone phonetic encoding algorithm.
//!
//! Caverphone was developed by David Hood at the University of Otago in New
//! Zealand for matching names in the electoral roll. It is tuned for New
//! Zealand English pronunciation.
//!
//! This implements Caverphone 2.0, which produces a 10-character code.
//!
//! The algorithm applies a sequence of ordered string replacements to the
//! lowercased input, then pads/truncates to 10 characters.

use super::PhoneticEncoder;

/// Caverphone 2.0 encoder.
pub struct Caverphone;

impl PhoneticEncoder for Caverphone {
    fn encode(&self, input: &str) -> String {
        let input = input.trim();
        if input.is_empty() {
            return String::new();
        }

        // Step 1: lowercase, keep only alpha
        let mut s: String = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_lowercase())
            .collect();

        if s.is_empty() {
            return String::new();
        }

        // Step 2: Remove trailing 'e'
        if s.ends_with('e') {
            s.pop();
            if s.is_empty() {
                return "1111111111".to_string();
            }
        }

        // Step 3: Handle starting patterns
        if s.starts_with("cough") {
            s = format!("cou2f{}", &s[5..]);
        } else if s.starts_with("rough") {
            s = format!("rou2f{}", &s[5..]);
        } else if s.starts_with("tough") {
            s = format!("tou2f{}", &s[5..]);
        } else if s.starts_with("enough") {
            s = format!("enou2f{}", &s[6..]);
        } else if s.starts_with("trough") {
            s = format!("trou2f{}", &s[6..]);
        }

        if s.starts_with("gn") {
            s = format!("2n{}", &s[2..]);
        }

        // Step 4: Handle ending 'mb'
        if s.ends_with("mb") {
            let new_len = s.len() - 2;
            s.truncate(new_len);
            s.push_str("m2");
        }

        // Step 5: Apply phonetic replacements in order
        s = s.replace("cq", "2q");
        s = s.replace("ci", "si");
        s = s.replace("ce", "se");
        s = s.replace("cy", "sy");
        s = s.replace("tch", "2ch");
        s = s.replace("c", "k");
        s = s.replace("q", "k");
        s = s.replace("x", "k");
        s = s.replace("v", "f");
        s = s.replace("dg", "2g");
        s = s.replace("tio", "sio");
        s = s.replace("tia", "sia");
        s = s.replace("d", "t");
        s = s.replace("ph", "fh");
        s = s.replace("b", "p");
        s = s.replace("sh", "s2");
        s = s.replace("z", "s");

        // Replace initial vowel with its mapped value
        // (this is done as: if starts with vowel, replace that vowel pattern)
        let first = s.chars().next().unwrap_or('\0');
        if matches!(first, 'a' | 'e' | 'i' | 'o' | 'u') {
            // Initial vowel is kept as the starting character
            // but all other vowels will be removed
        }

        // Replace remaining vowels: 'aeiou' -> '3' (except initial)
        s = s.replace("gh", "22");
        s = s.replace("h", "2");

        // Now handle vowels: keep first char, replace interior vowels with 3
        let mut chars: Vec<char> = s.chars().collect();
        for ch in chars.iter_mut().skip(1) {
            if matches!(*ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y') {
                *ch = '3';
            }
        }
        s = chars.into_iter().collect();

        // Handle w -> treat like vowel (replace with 3 after position 0)
        let mut chars: Vec<char> = s.chars().collect();
        for ch in chars.iter_mut().skip(1) {
            if *ch == 'w' {
                *ch = '3';
            }
        }
        s = chars.into_iter().collect();

        // Remove all '3's
        s = s.replace('3', "");

        // Remove all '2's
        s = s.replace('2', "");

        // If empty after removals, use initial character
        if s.is_empty() {
            s = first.to_string();
        }

        // Pad with '1's to length 10
        while s.len() < 10 {
            s.push('1');
        }
        s.truncate(10);

        s.to_ascii_uppercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_examples() {
        let c = Caverphone;
        // Known Caverphone 2.0 test cases
        let code = c.encode("Lee");
        assert_eq!(code.len(), 10);
        assert!(
            code.starts_with('L'),
            "Expected Lee to start with L, got {code}"
        );
    }

    #[test]
    fn empty_input() {
        let c = Caverphone;
        assert_eq!(c.encode(""), "");
        assert_eq!(c.encode("   "), "");
    }

    #[test]
    fn case_insensitive() {
        let c = Caverphone;
        assert_eq!(c.encode("smith"), c.encode("SMITH"));
        assert_eq!(c.encode("Thompson"), c.encode("thompson"));
    }

    #[test]
    fn ten_char_output() {
        let c = Caverphone;
        let code = c.encode("Smith");
        assert_eq!(code.len(), 10);
    }

    #[test]
    fn similar_names() {
        let c = Caverphone;
        // Similar sounding names should get the same code
        let code1 = c.encode("Smith");
        let code2 = c.encode("Smyth");
        assert_eq!(code1, code2, "Smith={code1} Smyth={code2} should match");
    }

    #[test]
    fn nz_names() {
        let c = Caverphone;
        // Lee and Lea should match (both end in 'e' which is stripped)
        let code1 = c.encode("Lee");
        let code2 = c.encode("Lea");
        assert_eq!(code1, code2, "Lee={code1} Lea={code2} should match");
    }

    #[test]
    fn single_letter() {
        let c = Caverphone;
        let code = c.encode("A");
        assert_eq!(code.len(), 10);
    }
}
