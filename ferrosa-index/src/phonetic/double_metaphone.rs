//! Double Metaphone encoding algorithm.
//!
//! Double Metaphone generates a primary and an alternate phonetic code for
//! a given input. The primary code represents the most common pronunciation;
//! the alternate handles variant pronunciations (e.g., for names of different
//! ethnic origins).
//!
//! For simplicity in v1, the [`PhoneticEncoder::encode`] implementation
//! returns only the primary code. The full `(primary, alternate)` pair is
//! available via [`DoubleMetaphoneEncoder::encode_full`].
//!
//! Reference: Lawrence Philips, "The Double Metaphone Search Algorithm" (2000).

use super::PhoneticEncoder;

/// Double Metaphone encoder.
pub struct DoubleMetaphoneEncoder;

impl DoubleMetaphoneEncoder {
    /// Maximum code length.
    const MAX_LEN: usize = 4;

    fn is_vowel(c: char) -> bool {
        matches!(c, 'A' | 'E' | 'I' | 'O' | 'U')
    }

    /// Return both primary and alternate metaphone codes.
    pub fn encode_full(&self, input: &str) -> (String, String) {
        let chars: Vec<char> = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_uppercase())
            .collect();

        if chars.is_empty() {
            return (String::new(), String::new());
        }

        let len = chars.len();
        let mut primary = String::with_capacity(Self::MAX_LEN);
        let mut alternate = String::with_capacity(Self::MAX_LEN);

        let at = |i: usize| -> char {
            if i < len {
                chars[i]
            } else {
                '\0'
            }
        };

        let mut i: usize = 0;

        // Handle initial silent letters
        match (at(0), at(1)) {
            ('G', 'N') | ('K', 'N') | ('P', 'N') | ('A', 'E') | ('W', 'R') => i = 1,
            _ => {}
        }

        // Initial vowel -> map to A
        if i == 0 && Self::is_vowel(at(0)) {
            primary.push('A');
            alternate.push('A');
            i = 1;
        }

        while i < len && (primary.len() < Self::MAX_LEN || alternate.len() < Self::MAX_LEN) {
            let c = at(i);
            let next = at(i + 1);
            let prev = if i > 0 { at(i - 1) } else { '\0' };

            // Skip duplicate letters except C
            if c == prev && c != 'C' {
                i += 1;
                continue;
            }

            let mut push_both = |ch: char| {
                if primary.len() < Self::MAX_LEN {
                    primary.push(ch);
                }
                if alternate.len() < Self::MAX_LEN {
                    alternate.push(ch);
                }
            };

            match c {
                'A' | 'E' | 'I' | 'O' | 'U' => {
                    // Vowels only kept at start (already handled above)
                }
                'B' => {
                    if !(i == len - 1 && prev == 'M') {
                        push_both('P');
                    }
                }
                'C' => {
                    if next == 'H' {
                        push_both('X');
                        i += 1;
                    } else if next == 'I' || next == 'E' || next == 'Y' {
                        if primary.len() < Self::MAX_LEN {
                            primary.push('S');
                        }
                        if alternate.len() < Self::MAX_LEN {
                            alternate.push('S');
                        }
                        if next == 'I' && at(i + 2) == 'A' {
                            i += 2;
                        }
                    } else {
                        push_both('K');
                    }
                }
                'D' => {
                    if next == 'G' && (at(i + 2) == 'I' || at(i + 2) == 'E' || at(i + 2) == 'Y') {
                        push_both('J');
                        i += 1;
                    } else {
                        push_both('T');
                    }
                }
                'F' => push_both('F'),
                'G' => {
                    if next == 'H' {
                        if i + 2 < len && Self::is_vowel(at(i + 2)) {
                            push_both('K');
                        }
                        i += 1;
                    } else if i > 0 && (next == 'N' || next == '\0') {
                        // Silent G at end or before N
                    } else if next == 'I' || next == 'E' || next == 'Y' {
                        // G before front vowel: primary=J, alternate=K
                        if primary.len() < Self::MAX_LEN {
                            primary.push('J');
                        }
                        if alternate.len() < Self::MAX_LEN {
                            alternate.push('K');
                        }
                    } else {
                        push_both('K');
                    }
                }
                'H' => {
                    if Self::is_vowel(next) && !Self::is_vowel(prev) {
                        push_both('H');
                    }
                }
                'J' => push_both('J'),
                'K' => {
                    if prev != 'C' {
                        push_both('K');
                    }
                }
                'L' => push_both('L'),
                'M' => push_both('M'),
                'N' => push_both('N'),
                'P' => {
                    if next == 'H' {
                        push_both('F');
                        i += 1;
                    } else {
                        push_both('P');
                    }
                }
                'Q' => push_both('K'),
                'R' => push_both('R'),
                'S' => {
                    if next == 'H' {
                        push_both('X');
                        i += 1;
                    } else if next == 'I' && (at(i + 2) == 'A' || at(i + 2) == 'O') {
                        push_both('X');
                        i += 2;
                    } else {
                        push_both('S');
                    }
                }
                'T' => {
                    if next == 'H' {
                        push_both('T');
                        i += 1;
                    } else if next == 'I' && (at(i + 2) == 'A' || at(i + 2) == 'O') {
                        push_both('X');
                        i += 2;
                    } else {
                        push_both('T');
                    }
                }
                'V' => push_both('F'),
                'W' | 'Y' => {
                    if Self::is_vowel(next) {
                        push_both(c);
                    }
                }
                'X' => {
                    if primary.len() < Self::MAX_LEN {
                        primary.push('K');
                    }
                    if alternate.len() < Self::MAX_LEN {
                        alternate.push('K');
                    }
                    if primary.len() < Self::MAX_LEN {
                        primary.push('S');
                    }
                    if alternate.len() < Self::MAX_LEN {
                        alternate.push('S');
                    }
                }
                'Z' => push_both('S'),
                _ => {}
            }

            i += 1;
        }

        primary.truncate(Self::MAX_LEN);
        alternate.truncate(Self::MAX_LEN);

        (primary, alternate)
    }
}

impl PhoneticEncoder for DoubleMetaphoneEncoder {
    fn encode(&self, input: &str) -> String {
        self.encode_full(input).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_metaphone_basic() {
        let enc = DoubleMetaphoneEncoder;
        let code = enc.encode("Smith");
        assert!(!code.is_empty(), "Smith should produce a code");
    }

    #[test]
    fn double_metaphone_empty() {
        let enc = DoubleMetaphoneEncoder;
        assert_eq!(enc.encode(""), "");
    }

    #[test]
    fn double_metaphone_full_returns_pair() {
        let enc = DoubleMetaphoneEncoder;
        let (primary, alternate) = enc.encode_full("Smith");
        assert!(!primary.is_empty());
        assert!(!alternate.is_empty());
    }

    #[test]
    fn double_metaphone_similar_names() {
        let enc = DoubleMetaphoneEncoder;
        assert_eq!(enc.encode("Smith"), enc.encode("Smythe"));
    }

    #[test]
    fn double_metaphone_ph() {
        let enc = DoubleMetaphoneEncoder;
        let code = enc.encode("Philip");
        assert!(
            code.starts_with('F'),
            "Philip should start with F, got: {code}"
        );
    }

    #[test]
    fn double_metaphone_max_length() {
        let enc = DoubleMetaphoneEncoder;
        let code = enc.encode("Alexandropoulos");
        assert!(
            code.len() <= 4,
            "code should be at most 4 chars, got: {code}"
        );
    }

    #[test]
    fn double_metaphone_case_insensitive() {
        let enc = DoubleMetaphoneEncoder;
        assert_eq!(enc.encode("SMITH"), enc.encode("smith"));
    }

    #[test]
    fn double_metaphone_g_variants() {
        let enc = DoubleMetaphoneEncoder;
        let (primary, alternate) = enc.encode_full("George");
        assert!(
            primary.starts_with('J'),
            "George primary should start with J, got: {primary}"
        );
        assert!(
            alternate.starts_with('K'),
            "George alternate should start with K, got: {alternate}"
        );
    }
}
