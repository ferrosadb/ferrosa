//! Property-based tests for ferrosa-schema.

use std::collections::HashMap;

use proptest::prelude::*;

use ferrosa_schema::*;

proptest! {
    #[test]
    fn schema_snapshot_serde_roundtrip(
        ks_name in "[a-z][a-z0-9_]{0,10}",
    ) {
        let mut snap = SchemaSnapshot::new();
        snap.keyspaces.insert(ks_name.clone(), KeyspaceMetadata {
            name: ks_name.clone(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        });
        let json = serde_json::to_string(&snap).unwrap();
        let back: SchemaSnapshot = serde_json::from_str(&json).unwrap();
        prop_assert!(back.keyspaces.contains_key(&ks_name));
    }

    #[test]
    fn superuser_always_authorized(
        perm_idx in 0..8usize,
    ) {
        let perms = [
            Permission::Create, Permission::Alter, Permission::Drop,
            Permission::Select, Permission::Modify, Permission::Authorize,
            Permission::Describe, Permission::Execute,
        ];
        let snap = SchemaSnapshot::new();
        let auth = AuthContext {
            role: "super".to_string(),
            is_superuser: true,
            must_change_password: false,
        };
        let result = check_permission(
            &snap, &auth, perms[perm_idx], &Resource::AllKeyspaces,
        );
        prop_assert!(result.is_ok());
    }
}

// Bcrypt property tests use reduced case count to keep runtime reasonable.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn hash_verifies_same_password(password in ".{1,50}") {
        let hasher = PasswordHasher::Bcrypt { cost: 4 };
        let hash = hasher.hash_password(&password).unwrap();
        prop_assert!(PasswordHasher::verify_password_any(&password, &hash).unwrap());
    }

    #[test]
    fn hash_rejects_different_password(
        password in ".{1,30}",
        other in ".{1,30}",
    ) {
        prop_assume!(password != other);
        let hasher = PasswordHasher::Bcrypt { cost: 4 };
        let hash = hasher.hash_password(&password).unwrap();
        prop_assert!(!PasswordHasher::verify_password_any(&other, &hash).unwrap());
    }
}
