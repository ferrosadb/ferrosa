//! Node boot hashes the three seed-role passwords before it reports readiness.
//!
//! If one hash costs seconds, that cost dominates a fresh node's boot, and any
//! CPU contention (a shared CI runner, a full test suite) stretches boot past the
//! readiness budget the cluster harnesses allow. The KDF parameters must stay
//! exactly as configured -- the fix is to compile the KDF optimised, not to
//! weaken it.

use std::time::Instant;

use ferrosa_schema::PasswordHasher;

/// Boot hashes three seed passwords; keeping each well under a second keeps a
/// fresh node's readiness measured in seconds even on a contended runner.
#[test]
fn a_single_default_hash_stays_within_the_boot_budget() {
    let hasher = PasswordHasher::default();
    let started = Instant::now();
    let hash = hasher
        .hash_password("well-known-development-seed-password")
        .expect("hash the seed password");
    let elapsed = started.elapsed();
    println!("one default hash: {} ms", elapsed.as_millis());
    assert!(
        !hash.is_empty(),
        "hashing the seed password must produce a hash"
    );
    assert!(
        elapsed.as_millis() < 900,
        "one default hash took {} ms; boot hashes three, so readiness is dominated \
         by it and contention pushes a fresh node past its readiness budget",
        elapsed.as_millis()
    );
}

/// Guards the fix: compiling the KDF optimised must not change the algorithm,
/// the cost factor, or the hash format -- only its speed.
#[test]
fn optimised_kdf_build_keeps_the_same_parameters_and_verification() {
    let hasher = PasswordHasher::default();
    let hash = hasher.hash_password("a-password").expect("hash");

    assert!(
        hash.starts_with("$2b$12$"),
        "expected bcrypt cost 12 in the encoded hash, got: {hash}"
    );
    assert!(
        PasswordHasher::verify_password_any("a-password", &hash).expect("verify"),
        "the correct password must verify against its own hash"
    );
    assert!(
        !PasswordHasher::verify_password_any("not-the-password", &hash).expect("verify"),
        "a wrong password must not verify"
    );
}
