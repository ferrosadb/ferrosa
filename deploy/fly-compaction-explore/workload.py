"""Pure, deterministic planning and payloads for compaction experiments."""

import hashlib
import random


def _require_int(name, value, minimum=None):
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")


def plan_phase(operations, read_fraction, keys, seed):
    """Return a shuffled plan with rounded read counts and uniform key draws.

    Counts must be positive integers, the fraction must be a finite int or
    float in [0, 1], and the seed must be an integer. Booleans are rejected.
    A private RNG keeps each call independent of global random state.
    """
    _require_int("operations", operations, 1)
    _require_int("keys", keys, 1)
    _require_int("seed", seed)
    if type(read_fraction) not in (int, float):
        raise TypeError("read_fraction must be an int or float")
    if not 0 <= read_fraction <= 1:
        raise ValueError("read_fraction must be finite and between 0 and 1")

    reads = round(operations * read_fraction)
    ops = ["read"] * reads + ["write"] * (operations - reads)
    rng = random.Random(seed)
    rng.shuffle(ops)
    return [{"op": op, "key": rng.randrange(keys)} for op in ops]


def value_for(key, value_bytes):
    """Return deterministic pseudorandom bytes for a nonnegative integer key."""
    _require_int("key", key, 0)
    _require_int("value_bytes", value_bytes, 1)
    return hashlib.shake_256(str(key).encode()).digest(value_bytes)
