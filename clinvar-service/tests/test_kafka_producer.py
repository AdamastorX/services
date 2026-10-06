"""The ``clinvar.ingestion.completed`` event is split when ``changedKeys`` is large.

Found live 2026-10-05: the first ingestion in six weeks scanned the whole
VCF and then failed publishing its completion event with
``MSG_SIZE_TOO_LARGE``, because ``changedKeys`` (every key that changed
since the previous release) did not fit in one Kafka message. These tests
need no broker: the splitting is pure, and ``publish()`` is exercised
against a fake librdkafka ``Producer``.
"""

from __future__ import annotations

import json
import uuid

from app import kafka_producer
from app.kafka_producer import MAX_EVENT_PAYLOAD_BYTES, IngestionCompletedEvent, IngestionEventProducer

# The same key format the real diff produces (app/diff.py's _make_key) and the
# schema pins: variantAnnotation:{chrom}:{pos}:{ref}:{alt}.
KEY = "variantAnnotation:{chrom}:{pos}:{ref}:{alt}"


def _keys(n: int) -> list[str]:
    return [KEY.format(chrom="17", pos=43_000_000 + i, ref="A", alt="G") for i in range(n)]


def _event(changed_keys: list[str]) -> IngestionCompletedEvent:
    return IngestionCompletedEvent(
        new_release_id=uuid.UUID("b5a62384-b49a-4df9-8c59-b2b4f154ef3a"),
        previous_release_id=uuid.UUID("11111111-2222-3333-4444-555555555555"),
        published_date="2026-10-04",
        variant_count=4_500_000,
        ingested_at="2026-10-05T03:20:00Z",
        changed_keys=changed_keys,
    )


def test_a_small_event_is_one_unchanged_payload() -> None:
    event = _event(_keys(10))
    assert event.to_json_chunks() == [event.to_json()]


def test_an_event_with_no_changed_keys_is_still_exactly_one_payload() -> None:
    event = _event([])
    chunks = event.to_json_chunks()
    assert chunks == [event.to_json()]
    assert json.loads(chunks[0])["changedKeys"] == []


def test_a_large_diff_is_split_and_every_payload_fits_the_budget() -> None:
    keys = _keys(100_000)  # ~4.6 MB of JSON, far past the broker's 1 MiB
    event = _event(keys)
    assert len(event.to_json().encode()) > 1_048_588  # the original failure

    chunks = event.to_json_chunks()

    assert len(chunks) > 1
    assert all(len(c.encode()) <= MAX_EVENT_PAYLOAD_BYTES for c in chunks)


def test_splitting_loses_no_key_and_keeps_their_order() -> None:
    keys = _keys(100_000)
    chunks = _event(keys).to_json_chunks()
    rebuilt = [k for c in chunks for k in json.loads(c)["changedKeys"]]
    assert rebuilt == keys


def test_every_chunk_is_a_complete_event_with_the_same_release_fields() -> None:
    chunks = _event(_keys(100_000)).to_json_chunks()
    fields = {"newReleaseId", "previousReleaseId", "publishedDate", "variantCount", "ingestedAt", "changedKeys"}
    first = json.loads(chunks[0])
    for c in chunks:
        parsed = json.loads(c)
        assert set(parsed) == fields  # same shape as the unchanged schema
        assert {k: v for k, v in parsed.items() if k != "changedKeys"} == {
            k: v for k, v in first.items() if k != "changedKeys"
        }


def test_the_budget_is_honoured_for_a_custom_limit() -> None:
    chunks = _event(_keys(500)).to_json_chunks(max_bytes=2_000)
    assert len(chunks) > 1
    assert all(len(c.encode()) <= 2_000 for c in chunks)


class _FakeProducer:
    def __init__(self, _config: dict) -> None:
        self.produced: list[bytes] = []
        self.flushed = False

    def produce(self, topic: str, value: bytes, callback=None) -> None:
        self.produced.append(value)

    def poll(self, _timeout: float) -> int:
        return 0

    def flush(self, timeout: float = 0) -> int:
        self.flushed = True
        return 0


def test_publish_sends_one_message_per_chunk_and_flushes(monkeypatch) -> None:
    monkeypatch.setattr(kafka_producer, "Producer", _FakeProducer)
    producer = IngestionEventProducer("kafka:9092", "clinvar.ingestion.completed")
    keys = _keys(100_000)

    producer.publish(_event(keys))

    fake = producer._producer  # noqa: SLF001
    assert len(fake.produced) > 1
    assert all(len(m) <= MAX_EVENT_PAYLOAD_BYTES for m in fake.produced)
    assert [k for m in fake.produced for k in json.loads(m)["changedKeys"]] == keys
    assert fake.flushed


def test_publish_of_a_small_event_sends_a_single_message(monkeypatch) -> None:
    monkeypatch.setattr(kafka_producer, "Producer", _FakeProducer)
    producer = IngestionEventProducer("kafka:9092", "clinvar.ingestion.completed")

    producer.publish(_event(_keys(3)))

    assert len(producer._producer.produced) == 1  # noqa: SLF001
