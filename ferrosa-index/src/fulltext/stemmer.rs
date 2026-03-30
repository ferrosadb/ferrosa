//! Porter stemmer (Step 1–5) for English text normalization.
//!
//! Reference: M.F. Porter, "An algorithm for suffix stripping",
//! Program, 14(3): 130-137, 1980.
//!
//! This is a faithful implementation of the classic algorithm sufficient for
//! FTS indexing. It is not a replacement for a production-grade NLP library.

// ── Public entry point ────────────────────────────────────────────────────────

/// Reduce `word` to its English stem using the Porter algorithm.
///
/// Returns a new `String`; the input is not modified. Words shorter than
/// three characters are returned unchanged.
pub fn porter_stem(word: &str) -> String {
    if word.len() < 3 {
        return word.to_string();
    }

    let mut chars: Vec<char> = word.chars().collect();

    step1a(&mut chars);
    step1b(&mut chars);
    step1c(&mut chars);
    step2(&mut chars);
    step3(&mut chars);
    step4(&mut chars);
    step5a(&mut chars);
    step5b(&mut chars);

    chars.iter().collect()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` if `chars[i]` is a vowel (a, e, i, o, u, or y after a consonant).
fn is_vowel(chars: &[char], i: usize) -> bool {
    match chars[i] {
        'a' | 'e' | 'i' | 'o' | 'u' => true,
        'y' => i > 0 && !is_vowel(chars, i - 1),
        _ => false,
    }
}

/// Measure *m*: count consonant-vowel sequences in `chars[..end]`.
fn measure(chars: &[char], end: usize) -> usize {
    let mut m = 0;
    let mut i = 0;
    // Skip leading consonants
    while i < end && !is_vowel(chars, i) {
        i += 1;
    }
    loop {
        // Skip vowel cluster
        while i < end && is_vowel(chars, i) {
            i += 1;
        }
        if i >= end {
            break;
        }
        m += 1;
        // Skip consonant cluster
        while i < end && !is_vowel(chars, i) {
            i += 1;
        }
    }
    m
}

/// Returns `true` if `chars[..end]` contains a vowel.
fn contains_vowel(chars: &[char], end: usize) -> bool {
    (0..end).any(|i| is_vowel(chars, i))
}

/// Returns `true` if `chars[..end]` ends with a double consonant.
fn ends_double_consonant(chars: &[char], end: usize) -> bool {
    if end < 2 {
        return false;
    }
    let a = chars[end - 1];
    let b = chars[end - 2];
    a == b && !is_vowel(chars, end - 1)
}

/// Returns `true` if `chars[..end]` ends in consonant-vowel-consonant where
/// the final consonant is not w, x, or y.
fn ends_cvc_clean(chars: &[char], end: usize) -> bool {
    if end < 3 {
        return false;
    }
    !is_vowel(chars, end - 3)
        && is_vowel(chars, end - 2)
        && !is_vowel(chars, end - 1)
        && !matches!(chars[end - 1], 'w' | 'x' | 'y')
}

/// Check if `chars` ends with `suffix` and return the stem length if it does.
fn ends_with(chars: &[char], suffix: &[char]) -> Option<usize> {
    let n = chars.len();
    let s = suffix.len();
    if n >= s && &chars[n - s..] == suffix {
        Some(n - s)
    } else {
        None
    }
}

/// Replace the suffix (detected by `stem_len`) with `replacement` in-place.
fn replace_suffix(chars: &mut Vec<char>, stem_len: usize, replacement: &[char]) {
    chars.truncate(stem_len);
    chars.extend_from_slice(replacement);
}

// ── Step 1a ───────────────────────────────────────────────────────────────────

fn step1a(chars: &mut Vec<char>) {
    let suffixes: &[(&[char], &[char])] = &[
        (&['s', 's', 'e', 's'], &['s', 's']),
        (&['i', 'e', 's'], &['i']),
        (&['s', 's'], &['s', 's']),
        (&['s'], &[]),
    ];
    for (suffix, replacement) in suffixes {
        if let Some(stem) = ends_with(chars, suffix) {
            replace_suffix(chars, stem, replacement);
            return;
        }
    }
}

// ── Step 1b ───────────────────────────────────────────────────────────────────

fn step1b(chars: &mut Vec<char>) {
    // (m>0) EED -> EE
    let eed: Vec<char> = "eed".chars().collect();
    let ee: Vec<char> = "ee".chars().collect();
    if let Some(stem) = ends_with(chars, &eed) {
        if measure(chars, stem) > 0 {
            replace_suffix(chars, stem, &ee);
        }
        return;
    }

    // (*v*) ED -> ""
    let ed: Vec<char> = "ed".chars().collect();
    if let Some(stem) = ends_with(chars, &ed) {
        if contains_vowel(chars, stem) {
            replace_suffix(chars, stem, &[]);
            step1b_post(chars);
        }
        return;
    }

    // (*v*) ING -> ""
    let ing: Vec<char> = "ing".chars().collect();
    if let Some(stem) = ends_with(chars, &ing) {
        if contains_vowel(chars, stem) {
            replace_suffix(chars, stem, &[]);
            step1b_post(chars);
        }
    }
}

fn step1b_post(chars: &mut Vec<char>) {
    let n = chars.len();

    // AT -> ATE
    let at: Vec<char> = "at".chars().collect();
    let ate: Vec<char> = "ate".chars().collect();
    if ends_with(chars, &at).is_some() {
        chars.extend_from_slice(&ate[2..3]); // append 'e'
        return;
    }

    // BL -> BLE
    let bl: Vec<char> = "bl".chars().collect();
    if ends_with(chars, &bl).is_some() {
        chars.push('e');
        return;
    }

    // IZ -> IZE
    let iz: Vec<char> = "iz".chars().collect();
    if ends_with(chars, &iz).is_some() {
        chars.push('e');
        return;
    }

    // Double consonant (not L, S, Z) -> remove last char
    if n >= 2 && ends_double_consonant(chars, n) && !matches!(chars[n - 1], 'l' | 's' | 'z') {
        chars.pop();
        return;
    }

    // (m=1 and *o) -> E
    if measure(chars, n) == 1 && ends_cvc_clean(chars, n) {
        chars.push('e');
    }
}

// ── Step 1c ───────────────────────────────────────────────────────────────────

fn step1c(chars: &mut [char]) {
    let n = chars.len();
    if n > 0 && chars[n - 1] == 'y' && contains_vowel(chars, n - 1) {
        chars[n - 1] = 'i';
    }
}

// ── Step 2 ───────────────────────────────────────────────────────────────────

fn step2(chars: &mut Vec<char>) {
    let rules: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("abli", "able"),
        ("alli", "al"),
        ("entli", "ent"),
        ("eli", "e"),
        ("ousli", "ous"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("iveness", "ive"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("biliti", "ble"),
    ];
    apply_m1_rules(chars, rules);
}

// ── Step 3 ───────────────────────────────────────────────────────────────────

fn step3(chars: &mut Vec<char>) {
    let rules: &[(&str, &str)] = &[
        ("icate", "ic"),
        ("ative", ""),
        ("alize", "al"),
        ("iciti", "ic"),
        ("ical", "ic"),
        ("ful", ""),
        ("ness", ""),
    ];
    apply_m1_rules(chars, rules);
}

// ── Step 4 ───────────────────────────────────────────────────────────────────

fn step4(chars: &mut Vec<char>) {
    let rules: &[(&str, &str)] = &[
        ("al", ""),
        ("ance", ""),
        ("ence", ""),
        ("er", ""),
        ("ic", ""),
        ("able", ""),
        ("ible", ""),
        ("ant", ""),
        ("ement", ""),
        ("ment", ""),
        ("ent", ""),
        ("ou", ""),
        ("ism", ""),
        ("ate", ""),
        ("iti", ""),
        ("ous", ""),
        ("ive", ""),
        ("ize", ""),
    ];

    // Special: (m>1 and (*S or *T)) ION -> ""
    let ion: Vec<char> = "ion".chars().collect();
    if let Some(stem) = ends_with(chars, &ion) {
        if measure(chars, stem) > 1 && matches!(chars.get(stem - 1), Some('s') | Some('t')) {
            replace_suffix(chars, stem, &[]);
            return;
        }
    }

    apply_m2_rules(chars, rules);
}

// ── Step 5a ──────────────────────────────────────────────────────────────────

fn step5a(chars: &mut Vec<char>) {
    let n = chars.len();
    if n == 0 {
        return;
    }
    if chars[n - 1] == 'e' {
        let m = measure(chars, n - 1);
        if m > 1 || (m == 1 && !ends_cvc_clean(chars, n - 1)) {
            chars.pop();
        }
    }
}

// ── Step 5b ──────────────────────────────────────────────────────────────────

fn step5b(chars: &mut Vec<char>) {
    let n = chars.len();
    if n >= 2 && ends_double_consonant(chars, n) && chars[n - 1] == 'l' && measure(chars, n) > 1 {
        chars.pop();
    }
}

// ── Shared rule applicators ───────────────────────────────────────────────────

/// Apply suffix-replacement rules where m > 0 (Steps 2 and 3).
fn apply_m1_rules(chars: &mut Vec<char>, rules: &[(&str, &str)]) {
    for (suffix, replacement) in rules {
        let sfx: Vec<char> = suffix.chars().collect();
        if let Some(stem) = ends_with(chars, &sfx) {
            if measure(chars, stem) > 0 {
                let rep: Vec<char> = replacement.chars().collect();
                replace_suffix(chars, stem, &rep);
                return;
            }
        }
    }
}

/// Apply suffix-replacement rules where m > 1 (Step 4).
fn apply_m2_rules(chars: &mut Vec<char>, rules: &[(&str, &str)]) {
    for (suffix, replacement) in rules {
        let sfx: Vec<char> = suffix.chars().collect();
        if let Some(stem) = ends_with(chars, &sfx) {
            if measure(chars, stem) > 1 {
                let rep: Vec<char> = replacement.chars().collect();
                replace_suffix(chars, stem, &rep);
                return;
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Test cases from the canonical Porter stemmer test file.
    #[test]
    fn porter_stem_canonical_cases() {
        let cases = [
            ("caresses", "caress"),
            ("ponies", "poni"),
            ("ties", "ti"),
            ("caress", "caress"),
            ("cats", "cat"),
            ("troubles", "troubl"),
            ("troubled", "troubl"),
            ("troubling", "troubl"),
            ("hopping", "hop"),
            ("tanned", "tan"),
            ("falling", "fall"),
            ("hissing", "hiss"),
            ("fizzing", "fizz"),
            ("failing", "fail"),
            ("filing", "file"),
            ("happy", "happi"),
            ("sky", "sky"),
            ("relational", "relat"),
            ("generalization", "gener"),
            ("electrical", "electr"),
            ("effective", "effect"),
            ("communicate", "commun"),
        ];
        for (input, expected) in cases {
            let result = porter_stem(input);
            assert_eq!(
                result, expected,
                "porter_stem({input:?}) = {result:?}, expected {expected:?}"
            );
        }
    }

    #[test]
    fn short_words_unchanged() {
        assert_eq!(porter_stem("a"), "a");
        assert_eq!(porter_stem("by"), "by");
    }

    #[test]
    fn empty_string_unchanged() {
        assert_eq!(porter_stem(""), "");
    }
}
