//! Soundex phonetic encoding algorithm.
//!
//! The American Soundex algorithm encodes a name into a letter followed by
//! three digits (e.g., "Robert" -> "R163", "Smith" -> "S530"). Names that
//! sound alike produce the same code.
//!
//! Rules:
//! 1. Retain the first letter.
//! 2. Map remaining consonants to digits (see [`SOUNDEX_MAP`]).
//! 3. Collapse adjacent identical digits.
//! 4. Remove vowels, H, W, Y.
//! 5. Pad or truncate to exactly 4 characters.

use super::PhoneticEncoder;

/// Standard American Soundex encoder.
pub struct Soundex;

impl Soundex {
    fn soundex_code(c: char) -> Option<char> {
        match c.to_ascii_uppercase() {
            'B' | 'F' | 'P' | 'V' => Some('1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
            'D' | 'T' => Some('3'),
            'L' => Some('4'),
            'M' | 'N' => Some('5'),
            'R' => Some('6'),
            _ => None, // A, E, I, O, U, H, W, Y
        }
    }
}

impl PhoneticEncoder for Soundex {
    fn encode(&self, input: &str) -> String {
        let input = input.trim();
        if input.is_empty() {
            return String::new();
        }

        let mut chars = input.chars().filter(|c| c.is_ascii_alphabetic());
        let first = match chars.next() {
            Some(c) => c.to_ascii_uppercase(),
            None => return String::new(),
        };

        let mut result = String::with_capacity(4);
        result.push(first);

        let mut last_code = Self::soundex_code(first);

        for c in chars {
            if result.len() >= 4 {
                break;
            }
            let code = Self::soundex_code(c);
            if let Some(digit) = code {
                if code != last_code {
                    result.push(digit);
                }
            }
            // H and W don't update last_code (they are "transparent")
            let upper = c.to_ascii_uppercase();
            if upper != 'H' && upper != 'W' {
                last_code = code;
            }
        }

        // Pad with zeros to length 4
        while result.len() < 4 {
            result.push('0');
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_examples() {
        let s = Soundex;
        assert_eq!(s.encode("Robert"), "R163");
        assert_eq!(s.encode("Rupert"), "R163");
        assert_eq!(s.encode("Smith"), "S530");
        assert_eq!(s.encode("Smythe"), "S530");
        assert_eq!(s.encode("Ashcraft"), "A261");
        assert_eq!(s.encode("Ashcroft"), "A261");
        assert_eq!(s.encode("Tymczak"), "T522");
        assert_eq!(s.encode("Pfister"), "P236");
    }

    #[test]
    fn single_letter() {
        let s = Soundex;
        assert_eq!(s.encode("A"), "A000");
        assert_eq!(s.encode("Z"), "Z000");
    }

    #[test]
    fn empty_input() {
        let s = Soundex;
        assert_eq!(s.encode(""), "");
        assert_eq!(s.encode("   "), "");
    }

    #[test]
    fn case_insensitive() {
        let s = Soundex;
        assert_eq!(s.encode("robert"), s.encode("ROBERT"));
        assert_eq!(s.encode("smith"), s.encode("SMITH"));
    }

    #[test]
    fn similar_sounding_names() {
        let s = Soundex;
        // These should produce the same code
        assert_eq!(s.encode("Smith"), s.encode("Smythe"));
        assert_eq!(s.encode("Robert"), s.encode("Rupert"));
    }

    #[test]
    fn hw_transparency() {
        let s = Soundex;
        // H and W between same-coded consonants should not separate them
        assert_eq!(s.encode("Ashcraft"), s.encode("Ashcroft"));
    }
}
