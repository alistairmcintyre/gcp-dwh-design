"""The registry rules that stop a topic quietly becoming un-erasable."""

from __future__ import annotations

from privacy import tombstones


def topic(**overrides) -> dict:
    base = {
        "name": "client.onboarding.v2",
        "key": "client_id",
        "erasure": "tombstone",
        "cleanup_policy": "compact",
        "max_compaction_delay_hours": 24,
        "pii": {"email": "contact"},
        "encrypt_pii": True,
    }
    base.update(overrides)
    return base


def test_the_shipped_registry_is_clean():
    assert tombstones.check_erasure_config(tombstones.load_topics()) == []


def test_every_topic_in_the_registry_declares_a_method():
    assert all(t.get("erasure") for t in tombstones.load_topics())


def test_missing_method_is_rejected():
    problems = tombstones.check_erasure_config([topic(erasure=None)])
    assert "no erasure method declared" in problems[0]


def test_tombstone_without_compaction_is_rejected():
    """The common mistake: publish a tombstone to a topic that only has time-based retention."""
    problems = tombstones.check_erasure_config([topic(cleanup_policy="delete")])
    assert any("cleanup_policy compact" in p for p in problems)


def test_tombstone_on_a_topic_keyed_by_something_else_is_rejected():
    problems = tombstones.check_erasure_config([topic(key="order_id")])
    assert any("keyed by client_id" in p for p in problems)


def test_tombstone_without_a_bounded_delay_is_rejected():
    """Leaving compaction timing to the cluster default is how a 30-day deadline is missed."""
    problems = tombstones.check_erasure_config([topic(max_compaction_delay_hours=None)])
    assert any("max_compaction_delay_hours" in p for p in problems)


def test_declared_pii_must_be_encrypted():
    problems = tombstones.check_erasure_config([topic(encrypt_pii=False)])
    assert any("not encrypt_pii" in p for p in problems)


def test_retain_must_name_a_lawful_basis():
    problems = tombstones.check_erasure_config(
        [topic(erasure="retain", key="trade_id", pii={}, encrypt_pii=False)]
    )
    assert any("lawful basis" in p for p in problems)


def test_none_is_rejected_when_the_topic_is_keyed_by_the_subject():
    problems = tombstones.check_erasure_config([topic(erasure="none", pii={}, encrypt_pii=False)])
    assert any("holds subjects" in p for p in problems)


def test_plan_covers_every_tombstone_topic_and_nothing_else():
    planned = tombstones.plan("cli-42")
    topics_by_name = {t["name"]: t for t in tombstones.load_topics()}

    assert planned, "the registry has tombstone topics, so a plan cannot be empty"
    for tombstone in planned:
        assert topics_by_name[tombstone.topic]["erasure"] == "tombstone"
        assert tombstone.key == "cli-42"
        assert tombstone.max_delay_hours > 0

    expected = {name for name, t in topics_by_name.items() if t["erasure"] == "tombstone"}
    assert {t.topic for t in planned} == expected


def test_consent_topic_compacts_faster_than_the_rest():
    """A withdrawn marketing consent has to reach the audiences quicker than a month."""
    delays = {t.topic: t.max_delay_hours for t in tombstones.plan("cli-1")}
    assert delays["marketing.preferences.v1"] < delays["client.onboarding.v2"]
