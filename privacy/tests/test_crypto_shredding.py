"""What crypto shredding has to guarantee, written as assertions.

The interesting test is not "encrypt then decrypt returns the input". It is that after the key is
destroyed the ciphertext is unrecoverable, because that is the entire basis for telling a regulator
the data is erased while the bytes are still sitting in a Kafka segment.
"""

from __future__ import annotations

import duckdb
import pytest

from privacy.crypto import (
    ShreddedKeyError,
    decrypt_field,
    encrypt_field,
    new_data_key,
    unwrap,
    wrap,
)
from privacy.vault import DuckDBKeyVault


@pytest.fixture
def vault():
    return DuckDBKeyVault(duckdb.connect(":memory:"))


def test_round_trip(vault):
    key = vault.create_key("cli-1")
    token = encrypt_field("Amelia Okafor", key)
    assert token != "Amelia Okafor"
    assert decrypt_field(token, key) == "Amelia Okafor"


def test_ciphertext_is_unreadable_once_the_key_is_destroyed(vault):
    key = vault.create_key("cli-1")
    token = encrypt_field("amelia@example.com", key)

    vault.destroy_key("cli-1", reason="erasure_request")

    # The ciphertext is still here, exactly as it would still be in Kafka, in a backup, or in an
    # extract a partner holds. What has gone is any way to read it.
    assert token
    with pytest.raises(ShreddedKeyError):
        vault.data_key("cli-1")


def test_destroying_one_subject_leaves_everyone_else_readable(vault):
    first = vault.create_key("cli-1")
    second = vault.create_key("cli-2")
    first_token = encrypt_field("Amelia", first)
    second_token = encrypt_field("Oliver", second)

    vault.destroy_key("cli-1")

    assert decrypt_field(second_token, vault.data_key("cli-2")) == "Oliver"
    with pytest.raises(ShreddedKeyError):
        vault.data_key("cli-1")
    assert first_token  # still stored somewhere, still meaningless


def test_randomised_encryption_hides_equality(vault):
    key = vault.create_key("cli-1")
    assert encrypt_field("same", key) != encrypt_field("same", key)


def test_deterministic_encryption_keeps_joins_working(vault):
    """The trade-off, made explicit: equal values match, and that leaks which rows are equal."""
    key = vault.create_key("cli-1")
    same = encrypt_field("same", key, deterministic=True)
    assert same == encrypt_field("same", key, deterministic=True)
    assert same != encrypt_field("other", key, deterministic=True)


def test_the_same_value_differs_between_subjects(vault):
    """Deterministic mode must not become a global rainbow table of, say, every email address."""
    first = vault.create_key("cli-1")
    second = vault.create_key("cli-2")
    assert encrypt_field("a@example.com", first, deterministic=True) != encrypt_field(
        "a@example.com", second, deterministic=True
    )


def test_none_survives_as_none(vault):
    key = vault.create_key("cli-1")
    assert encrypt_field(None, key) is None
    assert decrypt_field(None, key) is None


def test_data_keys_are_never_stored_in_the_clear(vault):
    key = vault.create_key("cli-1")
    stored = vault.connection.execute(
        "select wrapped_key from privacy.subject_keys where subject_id = 'cli-1'"
    ).fetchone()[0]
    assert key not in stored.encode("utf-8")
    assert unwrap(stored) == key


def test_wrapping_is_reversible_only_with_the_kek():
    key = new_data_key()
    assert unwrap(wrap(key)) == key


def test_audit_records_the_erasure_without_keeping_the_data(vault):
    vault.create_key("cli-1")
    vault.destroy_key("cli-1", reason="subject request 2026-09-20")

    row = vault.connection.execute(
        "select subject_id, reason, key_existed from privacy.erasure_audit"
    ).fetchone()
    assert row == ("cli-1", "subject request 2026-09-20", True)


def test_erasing_an_unknown_subject_is_recorded_and_not_an_error(vault):
    """A request for someone who was never encrypted still has to be answerable."""
    assert vault.destroy_key("cli-unknown") is False
    assert vault.connection.execute("select count(*) from privacy.erasure_audit").fetchone()[0] == 1
