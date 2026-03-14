//! Metaphone phonetic encoding algorithm.
//!
//! The original Metaphone algorithm (Lawrence Philips, 1990) produces a
//! variable-length phonetic code by applying English pronunciation rules.
//! It is more accurate than Soundex for English names.

use super::PhoneticEncoder;

/// Original Metaphone encoder.
pub struct Metaphone {
    /// Maximum code length (default: 4).
    pub max_length: usize,
}

impl Default for Metaphone {
    fn default() -> Self {
        Self { max_length: 4 }
    }
}

impl Metaphone {
    fn is_vowel(c: char) -> bool {
        matches!(c, 'A' | 'E' | 'I' | 'O' | 'U')
    }
}

impl PhoneticEncoder for Metaphone {
    fn encode(&self, input: &str) -> String {
        let input = input.trim();
        if input.is_empty() {
            return String::new();
        }

        let upper: Vec<char> = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_uppercase())
            .collect();

        if upper.is_empty() {
            return String::new();
        }

        let mut result = String::with_capacity(self.max_length);
        let len = upper.len();
        let mut i = 0;

        // Drop initial silent letters
        if len >= 2 {
            match (upper[0], upper[1]) {
                ('A', 'E') | ('G', 'N') | ('K', 'N') | ('P', 'N') | ('W', 'R') => {
                    i = 1;
                }
                _ => {}
            }
        }

        // Special handling for initial vowel: emit it
        if i == 0 && Self::is_vowel(upper[0]) {
            result.push(upper[0]);
            i = 1;
        }

        while i < len && result.len() < self.max_length {
            let c = upper[i];
            let next = if i + 1 < len {
                Some(upper[i + 1])
            } else {
                None
            };
            let prev = if i > 0 { Some(upper[i - 1]) } else { None };
            let next2 = if i + 2 < len {
                Some(upper[i + 2])
            } else {
                None
            };

            // Skip duplicate adjacent consonants (except C)
            if c != 'C' && prev == Some(c) {
                i += 1;
                continue;
            }

            match c {
                'B' => {
                    // Drop B if after M at end of word
                    if prev != Some('M') || i + 1 != len {
                        result.push('B');
                    }
                }
                'C' => {
                    if next == Some('I') && next2 == Some('A') {
                        // CIA -> X
                        result.push('X');
                        i += 2;
                    } else if next == Some('I') || next == Some('E') || next == Some('Y') {
                        result.push('S');
                        i += 1;
                    } else if next == Some('H') {
                        result.push('X');
                        i += 1;
                    } else {
                        result.push('K');
                    }
                }
                'D' => {
                    if next == Some('G')
                        && next2.is_some()
                        && (next2 == Some('I') || next2 == Some('E') || next2 == Some('Y'))
                    {
                        result.push('J');
                        i += 2;
                    } else {
                        result.push('T');
                    }
                }
                'F' => {
                    result.push('F');
                }
                'G' => {
                    if next == Some('H') && i + 2 < len && !Self::is_vowel(upper[i + 2]) {
                        // GH before non-vowel: silent
                        i += 1;
                    } else if i > 0 && next == Some('H') && i + 2 >= len {
                        // GH at end: silent
                        i += 1;
                    } else if i > 0
                        && next == Some('N')
                        && (i + 2 >= len
                            || (i + 3 >= len && next2.is_some() && upper.get(i + 2) == Some(&'E')))
                    {
                        // GN or GNE at end: silent
                    } else if prev == Some('G') {
                        // Already handled by double-letter skip, but just in case
                    } else if next == Some('I') || next == Some('E') || next == Some('Y') {
                        result.push('J');
                    } else if next != Some('H') || Self::is_vowel(next.unwrap_or('X')) {
                        result.push('K');
                    }
                }
                'H' => {
                    // H before a vowel and not after a vowel
                    if next.is_some()
                        && Self::is_vowel(next.unwrap())
                        && (prev.is_none() || !Self::is_vowel(prev.unwrap()))
                    {
                        result.push('H');
                    }
                }
                'J' => {
                    result.push('J');
                }
                'K' => {
                    if prev != Some('C') {
                        result.push('K');
                    }
                }
                'L' => {
                    result.push('L');
                }
                'M' => {
                    result.push('M');
                }
                'N' => {
                    result.push('N');
                }
                'P' => {
                    if next == Some('H') {
                        result.push('F');
                        i += 1;
                    } else {
                        result.push('P');
                    }
                }
                'Q' => {
                    result.push('K');
                }
                'R' => {
                    result.push('R');
                }
                'S' => {
                    if next == Some('C') && next2 == Some('H') {
                        // SCH -> SK
                        result.push('S');
                        result.push('K');
                        i += 2;
                    } else if next == Some('H')
                        || (next == Some('I') && (next2 == Some('O') || next2 == Some('A')))
                    {
                        result.push('X');
                        i += 1;
                    } else {
                        result.push('S');
                    }
                }
                'T' => {
                    if next == Some('H') {
                        result.push('0'); // theta
                        i += 1;
                    } else if next == Some('I') && (next2 == Some('A') || next2 == Some('O')) {
                        result.push('X');
                        i += 1;
                    } else {
                        result.push('T');
                    }
                }
                'V' => {
                    result.push('F');
                }
                'W' | 'Y' => {
                    // Only if followed by a vowel
                    if next.is_some() && Self::is_vowel(next.unwrap()) {
                        result.push(c);
                    }
                }
                'X' => {
                    result.push('K');
                    result.push('S');
                }
                'Z' => {
                    result.push('S');
                }
                _ => {
                    // Vowels and other characters are skipped (after initial)
                }
            }

            i += 1;
        }

        result.truncate(self.max_length);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_examples() {
        let m = Metaphone::default();
        // Common test cases for Metaphone
        assert_eq!(m.encode("Smith"), "SM0");
        // SCH -> SK, M, (I skipped), D->T => SKMT
        assert_eq!(m.encode("Schmidt"), "SKMT");
    }

    #[test]
    fn empty_input() {
        let m = Metaphone::default();
        assert_eq!(m.encode(""), "");
        assert_eq!(m.encode("   "), "");
    }

    #[test]
    fn single_letter() {
        let m = Metaphone::default();
        assert_eq!(m.encode("A"), "A");
        assert_eq!(m.encode("B"), "B");
    }

    #[test]
    fn case_insensitive() {
        let m = Metaphone::default();
        assert_eq!(m.encode("smith"), m.encode("SMITH"));
        assert_eq!(m.encode("john"), m.encode("JOHN"));
    }

    #[test]
    fn phone_combinations() {
        let m = Metaphone::default();
        // PH -> F
        assert_eq!(m.encode("Phone"), "FN");
        // TH -> 0 (theta), O skipped, M, A skipped, S => 0MS
        assert_eq!(m.encode("Thomas"), "0MS");
    }

    #[test]
    fn silent_initial_letters() {
        let m = Metaphone::default();
        // KN -> N
        assert_eq!(m.encode("Knight"), "NT");
        assert_eq!(m.encode("Knife"), "NF");
        // PN -> N
        assert_eq!(m.encode("Pneumonia"), "NMN");
        // WR -> R
        assert_eq!(m.encode("Write"), "RT");
    }

    #[test]
    fn trailing_mb() {
        let m = Metaphone::default();
        // MB at end: B is silent
        assert_eq!(m.encode("Lamb"), "LM");
        assert_eq!(m.encode("Dumb"), "TM");
    }

    #[test]
    fn similar_sounding() {
        let m = Metaphone::default();
        // These should produce the same or similar codes
        assert_eq!(m.encode("Smith"), m.encode("Smyth"));
    }
}
