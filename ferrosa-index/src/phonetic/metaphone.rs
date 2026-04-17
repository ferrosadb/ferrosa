//! Standard Metaphone encoding algorithm.
//!
//! Metaphone generates a phonetic code based on English pronunciation rules.
//! It is more accurate than Soundex for English names.
//!
//! Reference: Lawrence Philips, "Hanging on the Metaphone" (1990).

use super::PhoneticEncoder;

/// Standard Metaphone encoder.
pub struct MetaphoneEncoder;

impl MetaphoneEncoder {
    /// Maximum length of the generated code.
    const MAX_LEN: usize = 4;

    fn is_vowel(c: char) -> bool {
        matches!(c, 'A' | 'E' | 'I' | 'O' | 'U')
    }
}

impl PhoneticEncoder for MetaphoneEncoder {
    fn encode(&self, input: &str) -> String {
        let chars: Vec<char> = input
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .map(|c| c.to_ascii_uppercase())
            .collect();

        if chars.is_empty() {
            return String::new();
        }

        let len = chars.len();
        let mut result = String::with_capacity(Self::MAX_LEN);

        let at = |i: usize| -> char {
            if i < len {
                chars[i]
            } else {
                '\0'
            }
        };

        // Drop initial silent letter combinations
        let mut i: usize = 0;
        match (at(0), at(1)) {
            ('A', 'E') | ('G', 'N') | ('K', 'N') | ('P', 'N') | ('W', 'R') => i = 1,
            _ => {}
        }

        while i < len && result.len() < Self::MAX_LEN {
            let c = at(i);
            let prev = if i > 0 { at(i - 1) } else { '\0' };
            let next = at(i + 1);
            let next2 = at(i + 2);

            // Skip duplicate adjacent letters (except C)
            if c == prev && c != 'C' {
                i += 1;
                continue;
            }

            match c {
                // Vowels are only kept at the beginning
                'A' | 'E' | 'I' | 'O' | 'U' if result.is_empty() => {
                    result.push(c);
                }
                // B unless silent at end after M (e.g., "dumb")
                'B' if !(i == len - 1 && prev == 'M') => {
                    result.push('B');
                }
                'C' => {
                    if next == 'I' && next2 == 'A' {
                        result.push('X');
                        i += 2;
                    } else if next == 'I' || next == 'E' || next == 'Y' {
                        result.push('S');
                        i += 1;
                    } else if next == 'H' {
                        if prev == 'S' {
                            result.push('K');
                        } else {
                            result.push('X');
                        }
                        i += 1;
                    } else {
                        result.push('K');
                    }
                }
                'D' => {
                    if next == 'G' && (next2 == 'I' || next2 == 'E' || next2 == 'Y') {
                        result.push('J');
                        i += 1;
                    } else {
                        result.push('T');
                    }
                }
                'F' => {
                    result.push('F');
                }
                'G' => {
                    if next == 'H' && (i + 2 >= len || !Self::is_vowel(next2)) {
                        // GH not followed by vowel, or GH at end -> skip
                        i += 1;
                    } else if i > 0
                        && next == 'N'
                        && (i + 2 >= len || (next2 == 'E' && at(i + 3) == '\0'))
                    {
                        // GN or GNE at end -> skip
                    } else if prev == 'G' {
                        // GG -> skip second G
                    } else if i > 0 && (next == 'I' || next == 'E' || next == 'Y') {
                        result.push('J');
                    } else if next == '\0' && i > 0 {
                        // Silent G at end
                    } else {
                        result.push('K');
                    }
                }
                'H' if Self::is_vowel(next) && !Self::is_vowel(prev) => {
                    result.push('H');
                }
                'J' => {
                    result.push('J');
                }
                'K' if prev != 'C' => {
                    result.push('K');
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
                    if next == 'H' {
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
                    if next == 'H' || (next == 'I' && (next2 == 'A' || next2 == 'O')) {
                        result.push('X');
                        if next != 'H' {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    } else if next == 'C' && next2 == 'H' {
                        result.push('S');
                        result.push('K');
                        i += 2;
                    } else {
                        result.push('S');
                    }
                }
                'T' => {
                    if next == 'H' {
                        result.push('0'); // theta
                        i += 1;
                    } else if next == 'I' && (next2 == 'A' || next2 == 'O') {
                        result.push('X');
                        i += 2;
                    } else {
                        result.push('T');
                    }
                }
                'V' => {
                    result.push('F');
                }
                'W' | 'Y' if Self::is_vowel(next) => {
                    result.push(c);
                }
                'X' => {
                    result.push('K');
                    result.push('S');
                }
                'Z' => {
                    result.push('S');
                }
                _ => {}
            }

            i += 1;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metaphone_basic() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode("Smith"), "SM0");
        // SCH -> SK in standard Metaphone
        assert_eq!(enc.encode("Schmidt"), "SKMT");
    }

    #[test]
    fn metaphone_empty() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode(""), "");
    }

    #[test]
    fn metaphone_initial_silent() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode("Knight"), "NT");
        assert_eq!(enc.encode("Pneumonia"), "NMN");
        assert_eq!(enc.encode("Wright"), "RT");
    }

    #[test]
    fn metaphone_ph() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode("Phone"), "FN");
    }

    #[test]
    fn metaphone_th() {
        let enc = MetaphoneEncoder;
        // TH -> 0 (theta) in standard Metaphone
        assert_eq!(enc.encode("Thomas"), "0MS");
    }

    #[test]
    fn metaphone_case_insensitive() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode("SMITH"), enc.encode("smith"));
    }

    #[test]
    fn metaphone_similar_names() {
        let enc = MetaphoneEncoder;
        assert_eq!(enc.encode("Smith"), enc.encode("Smythe"));
    }

    #[test]
    fn metaphone_x_maps_to_ks() {
        let enc = MetaphoneEncoder;
        let code = enc.encode("Alex");
        assert!(code.contains("KS"), "Alex should contain KS, got: {code}");
    }
}
