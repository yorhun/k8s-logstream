from unittest.mock import patch

import processor
import pytest


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.operations = []

    def xack(self, stream, group, message_id):
        self.operations.append(("xack", stream, group, message_id))
        return self

    def hdel(self, key, message_id):
        self.operations.append(("hdel", key, message_id))
        return self

    def zadd(self, key, values):
        self.operations.append(("zadd", key, values))
        return self

    def xdel(self, stream, *message_ids):
        self.operations.append(("xdel", stream, *message_ids))
        return self

    def zrem(self, key, *message_ids):
        self.operations.append(("zrem", key, *message_ids))
        return self

    def execute(self):
        for operation in self.operations:
            if operation[0] == "xack":
                self.client.xack(*operation[1:])
            elif operation[0] == "hdel":
                self.client.hdel(*operation[1:])
            elif operation[0] == "zadd":
                self.client.zadd(*operation[1:])
            elif operation[0] == "xdel":
                self.client.xdel(*operation[1:])
            elif operation[0] == "zrem":
                self.client.zrem(*operation[1:])


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.pending = set()
        self.acked = []
        self.dead_letters = []
        self.claimed_pending = []
        self.acked_scores = {}
        self.deleted = []

    def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    def get(self, key):
        return self.values.get(key)

    def eval(self, script, number_of_keys, *args):
        if script == processor.COMPLETE_CLAIM_SCRIPT:
            key, message_id, _ttl = args
            if self.values.get(key) == message_id:
                self.values[key] = f"done:{message_id}"
                return 1
            return 0
        if script == processor.DEAD_LETTER_SCRIPT:
            (dlq, stream, attempts_key, dedup_key, _acked_key, message_id, log_id,
             fields, attempts, error, failed_at, group, acked_at) = args
            self.dead_letters.append({
                "source_id": message_id,
                "log_id": log_id,
                "fields": fields,
                "attempts": attempts,
                "error": error,
                "failed_at": failed_at,
            })
            self.xack(stream, group, message_id)
            self.hdel(attempts_key, message_id)
            self.acked_scores[message_id] = float(acked_at)
            if self.values.get(dedup_key) == message_id:
                del self.values[dedup_key]
            return 1
        raise AssertionError("Unexpected Redis script")

    def pipeline(self, transaction=True):
        return FakePipeline(self)

    def xack(self, stream, group, message_id):
        self.acked.append(message_id)
        self.pending.discard(message_id)
        return 1

    def hincrby(self, key, field, amount):
        self.hashes.setdefault(key, {})
        self.hashes[key][field] = self.hashes[key].get(field, 0) + amount
        return self.hashes[key][field]

    def hdel(self, key, field):
        self.hashes.get(key, {}).pop(field, None)

    def zadd(self, key, values):
        self.acked_scores.update(values)

    def zrangebyscore(self, key, min, max):
        cutoff = float(max)
        return [message_id for message_id, score in self.acked_scores.items() if score <= cutoff]

    def xdel(self, stream, *message_ids):
        self.deleted.extend(message_ids)

    def zrem(self, key, *message_ids):
        for message_id in message_ids:
            self.acked_scores.pop(message_id, None)

    def xautoclaim(self, stream, group, consumer, *, min_idle_time, start_id, count):
        self.claimed_pending.append((stream, group, consumer, min_idle_time, start_id, count))
        return "0-0", [("stale-1", {"log_id": "stale"})], []


def event(log_id="log-1", message="event"):
    return {"log_id": log_id, "timestamp": "2026-10-19T12:00:00+00:00", "message": message}


@pytest.mark.unit
def test_bulk_results_ack_successes_and_leave_failures_pending():
    redis_client = FakeRedis()
    redis_client.pending.update({"good-1", "bad-1"})
    results = {"good-1": None, "bad-1": "mapping error"}

    with patch.object(processor, "_bulk_index", return_value=results):
        processor.process_entries(
            redis_client,
            object(),
            [("good-1", event("good")), ("bad-1", event("bad"))],
        )

    assert redis_client.acked == ["good-1"]
    assert redis_client.pending == {"bad-1"}
    assert redis_client.values["logs:dedup:good"] == "done:good-1"


@pytest.mark.unit
def test_failed_entry_moves_to_dlq_after_bounded_attempts(monkeypatch):
    monkeypatch.setattr(processor, "MAX_RETRIES", 2)
    redis_client = FakeRedis()
    redis_client.pending.add("msg-1")
    failed_result = {"msg-1": "temporary indexing failure"}

    with patch.object(processor, "_bulk_index", return_value=failed_result):
        processor.process_entries(redis_client, object(), [("msg-1", event())])
        assert "msg-1" in redis_client.pending
        processor.process_entries(redis_client, object(), [("msg-1", event())])

    assert redis_client.acked == ["msg-1"]
    assert len(redis_client.dead_letters) == 1
    assert redis_client.dead_letters[0]["attempts"] == 2
    assert "logs:dedup:log-1" not in redis_client.values


@pytest.mark.unit
def test_repeated_log_id_is_indexed_as_a_distinct_duplicate_event():
    redis_client = FakeRedis()
    indexed = []

    def index(_client, entries, *, processor_pod):
        indexed.extend(entries)
        return {message_id: None for message_id, _fields, _duplicate in entries}

    with patch.object(processor, "_bulk_index", side_effect=index):
        processor.process_entries(redis_client, object(), [("msg-1", event())])
        processor.process_entries(redis_client, object(), [("msg-2", event(message="repeat"))])

    assert [entry[0] for entry in indexed] == ["msg-1", "msg-2"]
    assert [entry[2] for entry in indexed] == [False, True]
    assert redis_client.acked == ["msg-1", "msg-2"]


@pytest.mark.unit
def test_bulk_actions_use_stream_id_as_stable_document_id(monkeypatch):
    captured = []

    def streaming_bulk(_client, actions, **_kwargs):
        captured.extend(actions)
        yield True, {"index": {"_id": actions[0]["_id"], "status": 201}}

    monkeypatch.setattr(processor.helpers, "streaming_bulk", streaming_bulk)
    result = processor._bulk_index(
        object(), [("stream-1", event(), False)], processor_pod="pod-a"
    )

    assert result == {"stream-1": None}
    assert captured[0]["_id"] == "stream-1"
    assert captured[0]["_index"] == "logs-2026.10.19"


@pytest.mark.unit
def test_pending_recovery_uses_xautoclaim():
    redis_client = FakeRedis()

    recovered = processor._recover_pending(redis_client)

    assert recovered == [("stale-1", {"log_id": "stale"})]
    assert redis_client.claimed_pending[0][0:2] == (processor.STREAM_NAME, processor.GROUP_NAME)
    assert redis_client.claimed_pending[0][3] == processor.PENDING_IDLE_MS


@pytest.mark.unit
def test_retention_prunes_only_acknowledged_entries(monkeypatch):
    monkeypatch.setattr(processor, "STREAM_RETENTION_SECONDS", 60)
    redis_client = FakeRedis()
    redis_client.acked_scores = {"old-acked": 1.0, "recent-acked": 999.0}
    redis_client.pending.add("pending")
    monkeypatch.setattr(processor.time, "time", lambda: 1000.0)

    pruned = processor._prune_acknowledged(redis_client)

    assert pruned == 1
    assert redis_client.deleted == ["old-acked"]
    assert redis_client.pending == {"pending"}
