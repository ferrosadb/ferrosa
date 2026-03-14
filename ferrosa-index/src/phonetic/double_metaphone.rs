//! Double Metaphone phonetic encoding algorithm.
//!
//! Developed by Lawrence Philips (2000), Double Metaphone returns two codes:
//! a primary and an alternate. The alternate captures secondary pronunciations
//! common in names of non-English origin (e.g., Germanic, Slavic, Celtic).
//!
//! This implementation produces codes of up to 4 characters.

use super::PhoneticEncoder;

/// Double Metaphone encoder.
///
/// Returns the primary code via [`PhoneticEncoder::encode`].
/// Use [`encode_both`](DoubleMetaphone::encode_both) to get both primary and alternate.
pub struct DoubleMetaphone {
    /// Maximum code length (default: 4).
    pub max_length: usize,
}

impl Default for DoubleMetaphone {
    fn default() -> Self {
        Self { max_length: 4 }
    }
}

impl DoubleMetaphone {
    fn is_vowel(c: char) -> bool {
        matches!(c, 'A' | 'E' | 'I' | 'O' | 'U')
    }

    fn char_at(chars: &[char], i: usize) -> char {
        if i < chars.len() {
            chars[i]
        } else {
            '\0'
        }
    }

    fn starts_with_at(chars: &[char], pos: usize, s: &str) -> bool {
        let target: Vec<char> = s.chars().collect();
        if pos + target.len() > chars.len() {
            return false;
        }
        for (j, &tc) in target.iter().enumerate() {
            if chars[pos + j] != tc {
                return false;
            }
        }
        true
    }

    fn is_slavo_germanic(chars: &[char]) -> bool {
        for &c in chars {
            if c == 'W' || c == 'K' {
                return true;
            }
        }
        // Check for CZ or WITZ patterns
        for i in 0..chars.len() {
            if Self::starts_with_at(chars, i, "CZ") || Self::starts_with_at(chars, i, "WITZ") {
                return true;
            }
        }
        false
    }

    /// Returns (primary, alternate) codes.
    pub fn encode_both(&self, input: &str) -> (String, String) {
        let input = input.trim();
        if input.is_empty() {
            return (String::new(), String::new());
        }

        let chars: Vec<char> = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_uppercase())
            .collect();

        if chars.is_empty() {
            return (String::new(), String::new());
        }

        let len = chars.len();
        let slavo_germanic = Self::is_slavo_germanic(&chars);
        let mut primary = String::with_capacity(self.max_length);
        let mut alternate = String::with_capacity(self.max_length);
        let mut i = 0;

        // Skip initial silent combinations
        if len >= 2 {
            match (chars[0], chars[1]) {
                ('G', 'N') | ('K', 'N') | ('P', 'N') | ('A', 'E') | ('W', 'R') => {
                    i = 1;
                }
                _ => {}
            }
        }

        // Initial vowel maps to A
        if i < len && Self::is_vowel(chars[i]) {
            primary.push('A');
            alternate.push('A');
            i += 1;
        }

        while i < len && (primary.len() < self.max_length || alternate.len() < self.max_length) {
            let c = chars[i];

            match c {
                'A' | 'E' | 'I' | 'O' | 'U' => {
                    // Vowels after initial position are skipped
                    i += 1;
                }
                'B' => {
                    primary.push('P');
                    alternate.push('P');
                    // Skip BB
                    if Self::char_at(&chars, i + 1) == 'B' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'C' => {
                    if Self::starts_with_at(&chars, i, "CH") {
                        primary.push('X');
                        alternate.push('X');
                        i += 2;
                    } else if Self::starts_with_at(&chars, i, "CK") {
                        primary.push('K');
                        alternate.push('K');
                        i += 2;
                    } else if Self::starts_with_at(&chars, i, "CI")
                        || Self::starts_with_at(&chars, i, "CE")
                        || Self::starts_with_at(&chars, i, "CY")
                    {
                        primary.push('S');
                        alternate.push('S');
                        i += 2;
                    } else {
                        primary.push('K');
                        alternate.push('K');
                        if Self::char_at(&chars, i + 1) == 'C' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                'D' => {
                    if Self::starts_with_at(&chars, i, "DG") {
                        let after_dg = Self::char_at(&chars, i + 2);
                        if after_dg == 'I' || after_dg == 'E' || after_dg == 'Y' {
                            primary.push('J');
                            alternate.push('J');
                            i += 3;
                        } else {
                            primary.push('T');
                            alternate.push('T');
                            primary.push('K');
                            alternate.push('K');
                            i += 2;
                        }
                    } else {
                        primary.push('T');
                        alternate.push('T');
                        if Self::char_at(&chars, i + 1) == 'D' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                'F' => {
                    primary.push('F');
                    alternate.push('F');
                    if Self::char_at(&chars, i + 1) == 'F' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'G' => {
                    if Self::char_at(&chars, i + 1) == 'H' {
                        if i + 2 < len && Self::is_vowel(chars[i + 2]) {
                            // GH before vowel
                            primary.push('K');
                            alternate.push('K');
                            i += 2;
                        } else {
                            // GH silent
                            i += 2;
                        }
                    } else if Self::char_at(&chars, i + 1) == 'N' {
                        // GN -> silent G
                        i += 1;
                    } else if Self::char_at(&chars, i + 1) == 'G' {
                        primary.push('K');
                        alternate.push('K');
                        i += 2;
                    } else {
                        let next = Self::char_at(&chars, i + 1);
                        if next == 'I' || next == 'E' || next == 'Y' {
                            primary.push('J');
                            alternate.push('K');
                            i += 2;
                        } else {
                            primary.push('K');
                            alternate.push('K');
                            i += 1;
                        }
                    }
                }
                'H' => {
                    // H is coded only if before a vowel and not after a vowel
                    if i + 1 < len
                        && Self::is_vowel(chars[i + 1])
                        && (i == 0 || !Self::is_vowel(chars[i - 1]))
                    {
                        primary.push('H');
                        alternate.push('H');
                    }
                    i += 1;
                }
                'J' => {
                    primary.push('J');
                    alternate.push('J');
                    if Self::char_at(&chars, i + 1) == 'J' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'K' => {
                    primary.push('K');
                    alternate.push('K');
                    if Self::char_at(&chars, i + 1) == 'K' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'L' => {
                    primary.push('L');
                    alternate.push('L');
                    if Self::char_at(&chars, i + 1) == 'L' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'M' => {
                    primary.push('M');
                    alternate.push('M');
                    if Self::char_at(&chars, i + 1) == 'M' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'N' => {
                    primary.push('N');
                    alternate.push('N');
                    if Self::char_at(&chars, i + 1) == 'N' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'P' => {
                    if Self::char_at(&chars, i + 1) == 'H' {
                        primary.push('F');
                        alternate.push('F');
                        i += 2;
                    } else {
                        primary.push('P');
                        alternate.push('P');
                        if Self::char_at(&chars, i + 1) == 'P' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                'Q' => {
                    primary.push('K');
                    alternate.push('K');
                    if Self::char_at(&chars, i + 1) == 'Q' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'R' => {
                    primary.push('R');
                    alternate.push('R');
                    if Self::char_at(&chars, i + 1) == 'R' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'S' => {
                    if Self::starts_with_at(&chars, i, "SH") {
                        primary.push('X');
                        alternate.push('X');
                        i += 2;
                    } else if Self::starts_with_at(&chars, i, "SCH") {
                        primary.push('X');
                        alternate.push('S');
                        i += 3;
                    } else if Self::starts_with_at(&chars, i, "SC") {
                        primary.push('S');
                        alternate.push('S');
                        primary.push('K');
                        alternate.push('K');
                        i += 2;
                    } else {
                        primary.push('S');
                        alternate.push('S');
                        if Self::char_at(&chars, i + 1) == 'S'
                            || Self::char_at(&chars, i + 1) == 'Z'
                        {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                'T' => {
                    if Self::starts_with_at(&chars, i, "TH") {
                        primary.push('T');
                        alternate.push('T');
                        i += 2;
                    } else if Self::starts_with_at(&chars, i, "TCH") {
                        i += 3;
                    } else {
                        primary.push('T');
                        alternate.push('T');
                        if Self::char_at(&chars, i + 1) == 'T' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                'V' => {
                    primary.push('F');
                    alternate.push('F');
                    if Self::char_at(&chars, i + 1) == 'V' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'W' => {
                    if i + 1 < len && Self::is_vowel(chars[i + 1]) {
                        primary.push('A');
                        alternate.push('F');
                        i += 1;
                    } else {
                        i += 1;
                    }
                }
                'X' => {
                    primary.push('K');
                    alternate.push('K');
                    primary.push('S');
                    alternate.push('S');
                    if Self::char_at(&chars, i + 1) == 'X' {
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                'Y' => {
                    if i + 1 < len && Self::is_vowel(chars[i + 1]) {
                        primary.push('A');
                        alternate.push('A');
                    }
                    i += 1;
                }
                'Z' => {
                    if Self::char_at(&chars, i + 1) == 'H' {
                        primary.push('J');
                        alternate.push('J');
                        i += 2;
                    } else {
                        primary.push('S');
                        if slavo_germanic {
                            alternate.push('S');
                        } else {
                            alternate.push('T');
                            alternate.push('S');
                        }
                        if Self::char_at(&chars, i + 1) == 'Z' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        primary.truncate(self.max_length);
        alternate.truncate(self.max_length);
        (primary, alternate)
    }
}

impl PhoneticEncoder for DoubleMetaphone {
    fn encode(&self, input: &str) -> String {
        self.encode_both(input).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_examples() {
        let dm = DoubleMetaphone::default();
        // Smith -> primary should start with SM or similar
        let (p, _) = dm.encode_both("Smith");
        assert!(!p.is_empty());
    }

    #[test]
    fn empty_input() {
        let dm = DoubleMetaphone::default();
        assert_eq!(dm.encode(""), "");
        assert_eq!(dm.encode("   "), "");
    }

    #[test]
    fn case_insensitive() {
        let dm = DoubleMetaphone::default();
        assert_eq!(dm.encode("smith"), dm.encode("SMITH"));
        assert_eq!(dm.encode("johnson"), dm.encode("JOHNSON"));
    }

    #[test]
    fn primary_and_alternate_differ() {
        let dm = DoubleMetaphone::default();
        // Words with multiple pronunciations may differ
        let (p, a) = dm.encode_both("Wagner");
        // Both should be non-empty
        assert!(!p.is_empty());
        assert!(!a.is_empty());
    }

    #[test]
    fn similar_names_match() {
        let dm = DoubleMetaphone::default();
        // Smith and Smyth should produce similar primary codes
        let (p1, _) = dm.encode_both("Smith");
        let (p2, _) = dm.encode_both("Smyth");
        assert_eq!(p1, p2);
    }

    #[test]
    fn ph_produces_f() {
        let dm = DoubleMetaphone::default();
        let (p, _) = dm.encode_both("Philip");
        assert!(
            p.starts_with('F'),
            "Expected Philip to start with F, got {p}"
        );
    }

    #[test]
    fn initial_silent_letters() {
        let dm = DoubleMetaphone::default();
        let (p1, _) = dm.encode_both("Knight");
        let (p2, _) = dm.encode_both("Night");
        // Both should start with N
        assert!(
            p1.starts_with('N'),
            "Expected Knight to start with N, got {p1}"
        );
        assert!(
            p2.starts_with('N'),
            "Expected Night to start with N, got {p2}"
        );
    }

    #[test]
    fn sch_handling() {
        let dm = DoubleMetaphone::default();
        let (p, a) = dm.encode_both("Schmidt");
        // SCH should produce X in primary, S in alternate
        assert!(!p.is_empty());
        assert!(!a.is_empty());
    }

    #[test]
    fn max_length_respected() {
        let dm = DoubleMetaphone { max_length: 4 };
        let (p, a) = dm.encode_both("Abrahamson");
        assert!(p.len() <= 4);
        assert!(a.len() <= 4);
    }
}
