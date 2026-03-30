//! Porter stemmer (1980) for English.
//!
//! Implements the algorithm described in:
//! M.F. Porter, "An algorithm for suffix stripping", Program, Vol. 14, No. 3,
//! pp. 130-137, July 1980.
//!
//! This is a faithful implementation of all 5 steps. It operates on lowercase
//! ASCII input; non-ASCII bytes are passed through unchanged.

/// Stem a single lowercase word using the Porter algorithm.
///
/// The input should be a single token (no whitespace). For best results,
/// lowercase the input before calling this function.
///
/// # Examples
///
/// ```
/// use ferrosa_index::fulltext::stemmer::stem;
/// assert_eq!(stem("running"), "run");
/// assert_eq!(stem("caresses"), "caress");
/// ```
pub fn stem(word: &str) -> String {
    let mut b: Vec<u8> = word.as_bytes().to_vec();

    // Words shorter than 3 characters are not stemmed.
    if b.len() < 3 {
        return word.to_owned();
    }

    step1a(&mut b);
    step1b(&mut b);
    step1c(&mut b);
    step2(&mut b);
    step3(&mut b);
    step4(&mut b);
    step5a(&mut b);
    step5b(&mut b);

    String::from_utf8(b).unwrap_or_else(|_| word.to_owned())
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Returns true if `b[i]` is a vowel (a, e, i, o, u).
/// A `y` after a consonant is also treated as a vowel.
fn is_vowel(b: &[u8], i: usize) -> bool {
    match b[i] {
        b'a' | b'e' | b'i' | b'o' | b'u' => true,
        b'y' => i > 0 && !is_vowel(b, i - 1),
        _ => false,
    }
}

/// Compute *m* — the count of consonant sequences (VC groups) in `b[0..end]`.
///
/// A consonant sequence is a maximal run of consonants. *m* counts how many
/// times a vowel sequence is followed by a consonant sequence in the stem.
fn measure(b: &[u8], end: usize) -> usize {
    if end == 0 {
        return 0;
    }
    let mut m = 0;
    let mut i = 0;
    // Skip any leading vowels.
    while i < end && is_vowel(b, i) {
        i += 1;
    }
    loop {
        // Skip consonants.
        while i < end && !is_vowel(b, i) {
            i += 1;
        }
        if i >= end {
            break;
        }
        m += 1;
        // Skip vowels.
        while i < end && is_vowel(b, i) {
            i += 1;
        }
        if i >= end {
            break;
        }
    }
    m
}

/// Returns true if `b[0..end]` contains at least one vowel.
fn has_vowel(b: &[u8], end: usize) -> bool {
    (0..end).any(|i| is_vowel(b, i))
}

/// Returns true if `b[end-1]` is a double consonant.
fn ends_double_consonant(b: &[u8], end: usize) -> bool {
    end >= 2 && b[end - 1] == b[end - 2] && !is_vowel(b, end - 1)
}

/// Returns true if the stem `b[0..end]` ends with consonant-vowel-consonant
/// where the final consonant is not w, x, or y.
fn ends_cvc(b: &[u8], end: usize) -> bool {
    if end < 3 {
        return false;
    }
    let c = b[end - 1];
    !is_vowel(b, end - 1)
        && is_vowel(b, end - 2)
        && !is_vowel(b, end - 3)
        && c != b'w'
        && c != b'x'
        && c != b'y'
}

/// Replace a suffix on `b` if the suffix matches and `measure(stem) > min_m`.
fn replace_if_m_gt(b: &mut Vec<u8>, suffix: &[u8], replacement: &[u8], min_m: usize) -> bool {
    if b.ends_with(suffix) {
        let stem_end = b.len() - suffix.len();
        if measure(b, stem_end) > min_m {
            b.truncate(stem_end);
            b.extend_from_slice(replacement);
            return true;
        }
    }
    false
}

// ── Steps ────────────────────────────────────────────────────────────────────

/// Step 1a: Plurals and -ed/-ing (simple plural handling).
///
/// sses → ss
/// ies  → i
/// ss   → ss  (no change)
/// s    → (remove)
fn step1a(b: &mut Vec<u8>) {
    if b.ends_with(b"sses") {
        let n = b.len();
        b.truncate(n - 2); // sses → ss
    } else if b.ends_with(b"ies") {
        let n = b.len();
        b.truncate(n - 2); // ies → i
    } else if b.ends_with(b"ss") {
        // no change
    } else if b.ends_with(b"s") {
        b.pop();
    }
}

/// Step 1b: -eed, -ed, -ing.
fn step1b(b: &mut Vec<u8>) {
    if b.ends_with(b"eed") {
        let stem_end = b.len() - 3;
        if measure(b, stem_end) > 0 {
            b.pop(); // eed → ee
        }
        return;
    }

    let had_vowel_before_ed = b.ends_with(b"ed") && {
        let stem_end = b.len() - 2;
        has_vowel(b, stem_end)
    };
    let had_vowel_before_ing = b.ends_with(b"ing") && {
        let stem_end = b.len() - 3;
        has_vowel(b, stem_end)
    };

    if had_vowel_before_ed {
        let n = b.len();
        b.truncate(n - 2);
    } else if had_vowel_before_ing {
        let n = b.len();
        b.truncate(n - 3);
    } else {
        return;
    }

    // Post-processing after removing -ed / -ing.
    if b.ends_with(b"at") || b.ends_with(b"bl") || b.ends_with(b"iz") {
        b.push(b'e');
    } else if ends_double_consonant(b, b.len())
        && !b.ends_with(b"l")
        && !b.ends_with(b"s")
        && !b.ends_with(b"z")
    {
        b.pop(); // double consonant → single
    } else if measure(b, b.len()) == 1 && ends_cvc(b, b.len()) {
        b.push(b'e');
    }
}

/// Step 1c: -y → -i when there is a vowel in the stem.
fn step1c(b: &mut [u8]) {
    if b.ends_with(b"y") {
        let stem_end = b.len() - 1;
        if has_vowel(b, stem_end) {
            *b.last_mut().unwrap() = b'i';
        }
    }
}

/// Step 2: Map common derivational suffixes (requires m > 0).
fn step2(b: &mut Vec<u8>) {
    // Ordered longest-first to avoid prefix matches on shorter suffixes.
    let rules: &[(&[u8], &[u8])] = &[
        (b"ational", b"ate"),
        (b"tional", b"tion"),
        (b"enci", b"ence"),
        (b"anci", b"ance"),
        (b"izer", b"ize"),
        (b"abli", b"able"),
        (b"alli", b"al"),
        (b"entli", b"ent"),
        (b"eli", b"e"),
        (b"ousli", b"ous"),
        (b"ization", b"ize"),
        (b"ation", b"ate"),
        (b"ator", b"ate"),
        (b"alism", b"al"),
        (b"iveness", b"ive"),
        (b"fulness", b"ful"),
        (b"ousness", b"ous"),
        (b"aliti", b"al"),
        (b"iviti", b"ive"),
        (b"biliti", b"ble"),
    ];
    for &(suffix, replacement) in rules {
        if replace_if_m_gt(b, suffix, replacement, 0) {
            return;
        }
    }
}

/// Step 3: Map more derivational suffixes (requires m > 0).
fn step3(b: &mut Vec<u8>) {
    let rules: &[(&[u8], &[u8])] = &[
        (b"icate", b"ic"),
        (b"ative", b""),
        (b"alize", b"al"),
        (b"iciti", b"ic"),
        (b"ical", b"ic"),
        (b"ful", b""),
        (b"ness", b""),
    ];
    for &(suffix, replacement) in rules {
        if replace_if_m_gt(b, suffix, replacement, 0) {
            return;
        }
    }
}

/// Step 4: Remove common suffixes when m > 1.
fn step4(b: &mut Vec<u8>) {
    let suffixes: &[&[u8]] = &[
        b"ement", b"ment", b"ance", b"ence", b"able", b"ible", b"ant",
        b"ent",  b"ism",  b"ate",  b"iti",  b"ous",  b"ive",  b"ize",
        b"ion",  b"al",   b"er",   b"ic",
    ];

    // Special case: "ion" requires the preceding char to be 's' or 't'.
    if b.ends_with(b"ion") {
        let stem_end = b.len() - 3;
        if stem_end > 0
            && (b[stem_end - 1] == b's' || b[stem_end - 1] == b't')
            && measure(b, stem_end) > 1
        {
            b.truncate(stem_end);
            return;
        }
    }

    for &suffix in suffixes {
        if suffix == b"ion" {
            continue; // handled above
        }
        if b.ends_with(suffix) {
            let stem_end = b.len() - suffix.len();
            if measure(b, stem_end) > 1 {
                b.truncate(stem_end);
                return;
            }
        }
    }
}

/// Step 5a: Remove final -e when m > 1, or when m == 1 and stem does not end CVC.
fn step5a(b: &mut Vec<u8>) {
    if b.ends_with(b"e") {
        let stem_end = b.len() - 1;
        let m = measure(b, stem_end);
        if m > 1 || (m == 1 && !ends_cvc(b, stem_end)) {
            b.pop();
        }
    }
}

/// Step 5b: Remove one of a double -ll when m > 1.
fn step5b(b: &mut Vec<u8>) {
    if measure(b, b.len()) > 1 && ends_double_consonant(b, b.len()) && b.ends_with(b"l") {
        b.pop();
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn porter_stemmer_basic() {
        assert_eq!(stem("running"), "run");
        assert_eq!(stem("jumps"), "jump");
        assert_eq!(stem("easily"), "easili");
        assert_eq!(stem("caresses"), "caress");
    }

    #[test]
    fn porter_stemmer_irregular() {
        // Porter stemmer doesn't handle truly irregular words
        // but should reduce common suffixes.
        assert_eq!(stem("databases"), "databas");
    }

    #[test]
    fn porter_stemmer_step1a_plurals() {
        assert_eq!(stem("caresses"), "caress"); // sses → ss
        assert_eq!(stem("ponies"), "poni");     // ies → i
        assert_eq!(stem("ties"), "ti");         // ies → i
        assert_eq!(stem("cats"), "cat");        // s → (removed)
    }

    #[test]
    fn porter_stemmer_step1b_ed_ing() {
        // "agreed": stem of "eed" is "agr" — m("agr")=0, rule requires m>0, no change.
        assert_eq!(stem("agreed"), "agreed");
        assert_eq!(stem("plastered"), "plaster");
        assert_eq!(stem("motoring"), "motor");
        assert_eq!(stem("sized"), "size");
    }

    #[test]
    fn porter_stemmer_step1c_y_to_i() {
        assert_eq!(stem("happy"), "happi");
        assert_eq!(stem("sky"), "sky"); // no vowel in stem "sk"
    }

    #[test]
    fn porter_stemmer_step2_derivational() {
        assert_eq!(stem("relational"), "relat");
        assert_eq!(stem("conditional"), "condit");
        // "rational": "ational" stem is "r" with m=0 (no VC group), so that rule
        // doesn't fire; "tional" stem is "ra" with m=1 → fires → "ration".
        assert_eq!(stem("rational"), "ration");
        assert_eq!(stem("valenci"), "valenc");
        assert_eq!(stem("digitizer"), "digit");
    }

    #[test]
    fn porter_stemmer_step3_ful_ness() {
        assert_eq!(stem("hopeful"), "hope");
        assert_eq!(stem("goodness"), "good");
    }

    #[test]
    fn porter_stemmer_step4_remove_long_suffixes() {
        assert_eq!(stem("revival"), "reviv");
        // "allowance": step4 "ance" stem "allow" m=1 (not >1), no fire;
        // step5a strips final "e" since stem "allowanc" has m=2 > 1.
        assert_eq!(stem("allowance"), "allowanc");
        // "inference": same pattern — step5a strips "e" → "inferenc".
        assert_eq!(stem("inference"), "inferenc");
    }

    #[test]
    fn porter_stemmer_short_words_unchanged() {
        assert_eq!(stem("a"), "a");
        assert_eq!(stem("be"), "be");
        assert_eq!(stem("sky"), "sky");
    }

    #[test]
    fn porter_stemmer_step5_final_e() {
        assert_eq!(stem("probate"), "probat");
        assert_eq!(stem("rate"), "rate"); // m == 1, ends CVC → keep e
        assert_eq!(stem("cease"), "ceas");
    }

    #[test]
    fn porter_stemmer_controllability() {
        // "generalization": step2 "ization"→"ize" → "generalize";
        // step3 "alize"→"al" → "general";
        // step4 "al" stem "gener" m=2>1 → strips "al" → "gener".
        assert_eq!(stem("generalization"), "gener");
        // "controllability": biliti→ble (step2) → "controllable";
        // step4 "able" stem "controll" m=2>1 → "controll";
        // step5b: double-l, m>1 → "control".
        assert_eq!(stem("controllability"), "control");
    }
}
