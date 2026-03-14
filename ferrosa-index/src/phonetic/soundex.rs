//! Standard Soundex encoding algorithm.
//!
//! Soundex maps names to a 4-character code: the first letter (uppercased)
//! followed by three digits derived from consonant groups. Vowels and
//! H/W/Y are dropped; adjacent identical codes are collapsed.
//!
//! Reference: <https://en.wikipedia.org/wiki/Soundex>

use super::PhoneticEncoder;

/// Standard Soundex encoder.
pub struct SoundexEncoder;

impl SoundexEncoder {
    /// Map a character to its Soundex digit, or `0` for letters that are dropped.
    fn soundex_code(c: char) -> Option<char> {
        match c.to_ascii_uppercase() {
            'B' | 'F' | 'P' | 'V' => Some('1'),
            'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
            'D' | 'T' => Some('3'),
            'L' => Some('4'),
            'M' | 'N' => Some('5'),
            'R' => Some('6'),
            'A' | 'E' | 'I' | 'O' | 'U' | 'H' | 'W' | 'Y' => Some('0'),
            _ => None,
        }
    }
}

impl PhoneticEncoder for SoundexEncoder {
    fn encode(&self, input: &str) -> String {
        // Filter to ASCII letters only
        let chars: Vec<char> = input.chars().filter(|c| c.is_ascii_alphabetic()).collect();
        if chars.is_empty() {
            return String::new();
        }

        let mut result = String::with_capacity(4);
        // First letter is retained as uppercase
        result.push(chars[0].to_ascii_uppercase());

        // The code of the first letter (used to skip duplicates)
        let first_code = Self::soundex_code(chars[0]);

        let mut last_code = first_code;
        for &c in &chars[1..] {
            if result.len() >= 4 {
                break;
            }
            let code = Self::soundex_code(c);
            match code {
                Some('0') => {
                    // Vowels and H/W/Y: H and W do NOT separate identical codes
                    // per the standard algorithm, but vowels do.
                    let upper = c.to_ascii_uppercase();
                    if upper != 'H' && upper != 'W' {
                        last_code = Some('0');
                    }
                }
                Some(digit) => {
                    if Some(digit) != last_code {
                        result.push(digit);
                    }
                    last_code = Some(digit);
                }
                None => {}
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
    fn soundex_standard_cases() {
        let enc = SoundexEncoder;
        assert_eq!(enc.encode("Robert"), "R163");
        assert_eq!(enc.encode("Rupert"), "R163");
        assert_eq!(enc.encode("Smith"), "S530");
        assert_eq!(enc.encode("Smythe"), "S530");
        assert_eq!(enc.encode("Ashcraft"), "A261");
        assert_eq!(enc.encode(""), "");
    }

    #[test]
    fn soundex_padding() {
        let enc = SoundexEncoder;
        assert_eq!(enc.encode("A"), "A000");
        assert_eq!(enc.encode("Al"), "A400");
    }

    #[test]
    fn soundex_truncation() {
        let enc = SoundexEncoder;
        let code = enc.encode("Washington");
        assert_eq!(code.len(), 4);
        assert_eq!(code, "W252");
    }

    #[test]
    fn soundex_adjacent_same_codes() {
        let enc = SoundexEncoder;
        // P and F both map to 1, should collapse
        assert_eq!(enc.encode("Pfister"), "P236");
    }

    #[test]
    fn soundex_hw_rule() {
        let enc = SoundexEncoder;
        // H does not separate S and C (both code 2), so the second 2 is dropped
        assert_eq!(enc.encode("Ashcraft"), "A261");
    }

    #[test]
    fn soundex_case_insensitive() {
        let enc = SoundexEncoder;
        assert_eq!(enc.encode("SMITH"), enc.encode("smith"));
        assert_eq!(enc.encode("Smith"), enc.encode("sMiTh"));
    }

    #[test]
    fn soundex_tymczak() {
        let enc = SoundexEncoder;
        assert_eq!(enc.encode("Tymczak"), "T522");
    }
}
