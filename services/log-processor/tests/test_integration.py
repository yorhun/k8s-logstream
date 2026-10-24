import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
import redis
from opensearchpy import OpenSearch

import processor


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def redis_service():
    redis_url = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
    redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        redis_client.ping()
    except Exception as exc:
        pytest.skip(f"Redis integration service is required: {exc}")
    return redis_client


@pytest.fixture(scope="module")
def service_clients(redis_service):
    opensearch_url = os.environ.get("TEST_OPENSEARCH_URL", "http://localhost:9200")
    os_client = OpenSearch(opensearch_url)
    try:
        available = os_client.ping()
    except Exception as exc:
        pytest.skip(f"OpenSearch integration service is required: {exc}")
    if not available:
        pytest.skip("OpenSearch integration service is unavailable")
    return redis_service, os_client


@pytest.mark.integration
def test_reclaimed_pending_message_is_indexed_and_acked(service_clients, monkeypatch):
    redis_client, os_client = service_clients
    suffix = uuid.uuid4().hex
    stream = f"test:logs:{suffix}"
    group = f"test-group:{suffix}"
    log_id = f"test-log:{suffix}"
    processor.STREAM_NAME = stream
    processor.GROUP_NAME = group
    processor.DLQ_STREAM_NAME = f"test:dead:{suffix}"
    index_name = "logs-integration-test"

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        message_id = redis_client.xadd(stream, {
            "log_id": log_id,
            "timestamp": "2026-10-19T12:00:00+00:00",
            "message": "integration test",
        })
        redis_client.xreadgroup(group, "stopped-consumer", {stream: ">"})
        monkeypatch.setattr(processor, "PENDING_IDLE_MS", 0)
        monkeypatch.setattr(processor, "_pending_cursor", "0-0")
        recovered = processor._recover_pending(redis_client)
        assert recovered[0][0] == message_id

        def index_with_test_index(client, entries, *, processor_pod):
            for item_id, fields, is_duplicate in entries:
                response = client.index(
                    index=index_name,
                    id=item_id,
                    body={**fields, "is_duplicate": is_duplicate},
                )
                assert response["result"] in {"created", "updated"}
            return {item_id: None for item_id, _fields, _duplicate in entries}

        monkeypatch.setattr(processor, "_bulk_index", index_with_test_index)
        processor.process_entries(redis_client, os_client, recovered, processor_pod="integration")

        os_client.indices.refresh(index=index_name)
        assert redis_client.xpending(stream, group)["pending"] == 0
        assert os_client.count(index=index_name, body={"query": {"ids": {"values": [message_id]}}})["count"] == 1
        assert os_client.get(index=index_name, id=message_id)["_source"]["is_duplicate"] is False
    finally:
        redis_client.delete(stream, processor._dedup_key(log_id), processor.ATTEMPTS_KEY)
        os_client.indices.delete(index=index_name, ignore_unavailable=True)


@pytest.mark.integration
def test_opensearch_bulk_reports_partial_item_failure(service_clients):
    _redis_client, os_client = service_clients
    index_name = f"bulk-failure-test-{uuid.uuid4().hex}"
    os_client.indices.create(
        index=index_name,
        body={"mappings": {"properties": {"level": {"type": "integer"}}}},
    )
    try:
        actions = [
            {"_op_type": "index", "_index": index_name, "_id": "valid", "_source": {"level": 1}},
            {"_op_type": "index", "_index": index_name, "_id": "invalid", "_source": {"level": "INFO"}},
        ]
        results = list(processor.helpers.streaming_bulk(
            os_client, actions, raise_on_error=False, raise_on_exception=False
        ))

        assert [ok for ok, _item in results] == [True, False]
        os_client.indices.refresh(index=index_name)
        assert os_client.count(index=index_name)["count"] == 1
    finally:
        os_client.indices.delete(index=index_name, ignore_unavailable=True)


@pytest.mark.integration
def test_concurrent_claims_allow_only_one_primary(redis_service):
    redis_client = redis_service
    log_id = f"test-log:{uuid.uuid4().hex}"
    barrier = threading.Barrier(8)

    def claim(message_number):
        barrier.wait(timeout=5)
        return processor._claim_message(redis_client, log_id, f"message-{message_number}")

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            classifications = list(executor.map(claim, range(8)))

        assert classifications.count("primary") == 1
        assert classifications.count("defer") == 7
    finally:
        redis_client.delete(processor._dedup_key(log_id))


@pytest.mark.integration
def test_processing_resumes_after_simulated_interruptions(redis_service, monkeypatch):
    redis_client = redis_service
    suffix = uuid.uuid4().hex
    stream = f"test:logs:{suffix}"
    group = f"test-group:{suffix}"
    monkeypatch.setattr(processor, "STREAM_NAME", stream)
    monkeypatch.setattr(processor, "GROUP_NAME", group)
    monkeypatch.setattr(processor, "PENDING_IDLE_MS", 0)
    monkeypatch.setattr(processor, "_pending_cursor", "0-0")
    fields_before_index = {"log_id": f"before-index:{suffix}"}
    fields_before_ack = {"log_id": f"before-ack:{suffix}"}
    fields_after_claim = {"log_id": f"after-claim:{suffix}"}
    indexed_documents = {}
    index_attempts = {}
    message_ids = []

    def successful_index(_client, entries, *, processor_pod):
        for message_id, fields, is_duplicate in entries:
            indexed_documents[message_id] = {**fields, "is_duplicate": is_duplicate}
            index_attempts[message_id] = index_attempts.get(message_id, 0) + 1
        return {message_id: None for message_id, _fields, _duplicate in entries}

    def crash_before_ack(*_args):
        raise RuntimeError("simulated processor crash after indexing")

    def crash_after_claim(*_args):
        raise RuntimeError("simulated processor crash after completing claim")

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        before_index_id = redis_client.xadd(stream, fields_before_index)
        before_ack_id = redis_client.xadd(stream, fields_before_ack)
        after_claim_id = redis_client.xadd(stream, fields_after_claim)
        message_ids.extend((before_index_id, before_ack_id, after_claim_id))

        redis_client.xreadgroup(group, "crashed-consumer", {stream: ">"}, count=1)
        assert processor._claim_message(
            redis_client, fields_before_index["log_id"], before_index_id
        ) == "primary"
        monkeypatch.setattr(processor, "_bulk_index", successful_index)
        processor.process_entries(
            redis_client,
            object(),
            [(before_index_id, fields_before_index)],
            processor_pod="restarted",
        )

        redis_client.xreadgroup(group, "crashed-consumer", {stream: ">"}, count=1)
        with monkeypatch.context() as crashed:
            crashed.setattr(processor, "_bulk_index", successful_index)
            crashed.setattr(processor, "_complete_claim", crash_before_ack)
            with pytest.raises(RuntimeError, match="simulated processor crash after indexing"):
                processor.process_entries(
                    redis_client,
                    object(),
                    [(before_ack_id, fields_before_ack)],
                    processor_pod="crashed",
                )

        assert redis_client.xpending(stream, group)["pending"] == 1
        assert redis_client.get(processor._dedup_key(fields_before_ack["log_id"])) == before_ack_id

        recovered = processor._recover_pending(redis_client)
        assert [message_id for message_id, _fields in recovered] == [before_ack_id]
        processor.process_entries(
            redis_client, object(), recovered, processor_pod="restarted"
        )

        redis_client.xreadgroup(group, "crashed-consumer", {stream: ">"}, count=1)
        with monkeypatch.context() as crashed:
            crashed.setattr(processor, "_bulk_index", successful_index)
            crashed.setattr(processor, "_ack_success", crash_after_claim)
            with pytest.raises(RuntimeError, match="after completing claim"):
                processor.process_entries(
                    redis_client,
                    object(),
                    [(after_claim_id, fields_after_claim)],
                    processor_pod="crashed",
                )

        assert redis_client.xpending(stream, group)["pending"] == 1
        assert redis_client.get(processor._dedup_key(fields_after_claim["log_id"])) == f"done:{after_claim_id}"
        recovered = processor._recover_pending(redis_client)
        assert [message_id for message_id, _fields in recovered] == [after_claim_id]
        processor.process_entries(
            redis_client, object(), recovered, processor_pod="restarted"
        )

        assert redis_client.xpending(stream, group)["pending"] == 0
        assert index_attempts == {
            before_index_id: 1,
            before_ack_id: 2,
            after_claim_id: 2,
        }
        assert set(indexed_documents) == {before_index_id, before_ack_id, after_claim_id}
        assert redis_client.get(processor._dedup_key(fields_before_ack["log_id"])) == f"done:{before_ack_id}"
        assert redis_client.get(processor._dedup_key(fields_after_claim["log_id"])) == f"done:{after_claim_id}"
    finally:
        redis_client.delete(
            stream,
            processor._dedup_key(fields_before_index["log_id"]),
            processor._dedup_key(fields_before_ack["log_id"]),
            processor._dedup_key(fields_after_claim["log_id"]),
        )
        if message_ids:
            redis_client.hdel(processor.ATTEMPTS_KEY, *message_ids)
            redis_client.zrem(processor.ACKED_KEY, *message_ids)


@pytest.mark.integration
def test_pending_recovery_continues_across_autoclaim_pages(redis_service, monkeypatch):
    redis_client = redis_service
    suffix = uuid.uuid4().hex
    stream = f"test:logs:{suffix}"
    group = f"test-group:{suffix}"
    monkeypatch.setattr(processor, "STREAM_NAME", stream)
    monkeypatch.setattr(processor, "GROUP_NAME", group)
    monkeypatch.setattr(processor, "PENDING_IDLE_MS", 0)
    monkeypatch.setattr(processor, "_pending_cursor", "0-0")

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        expected_ids = [
            redis_client.xadd(stream, {"log_id": f"log-{number}"})
            for number in range(101)
        ]
        delivered = redis_client.xreadgroup(
            group, "stopped-consumer", {stream: ">"}, count=101
        )
        assert len(delivered[0][1]) == 101

        first_page = processor._recover_pending(redis_client)
        assert len(first_page) == 100
        assert processor._pending_cursor != "0-0"

        second_page = processor._recover_pending(redis_client)
        assert len(second_page) == 1
        assert processor._pending_cursor == "0-0"
        assert {message_id for message_id, _fields in first_page + second_page} == set(expected_ids)
    finally:
        redis_client.delete(stream)


@pytest.mark.integration
def test_dead_lettering_moves_failed_message_and_cleans_state(redis_service, monkeypatch):
    redis_client = redis_service
    suffix = uuid.uuid4().hex
    stream = f"test:logs:{suffix}"
    group = f"test-group:{suffix}"
    dead_letter_stream = f"test:dead:{suffix}"
    log_id = f"test-log:{suffix}"
    monkeypatch.setattr(processor, "STREAM_NAME", stream)
    monkeypatch.setattr(processor, "GROUP_NAME", group)
    monkeypatch.setattr(processor, "DLQ_STREAM_NAME", dead_letter_stream)
    monkeypatch.setattr(processor, "MAX_RETRIES", 1)
    fields = {"log_id": log_id, "message": "mapping failure"}
    message_id = None

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        message_id = redis_client.xadd(stream, fields)
        redis_client.xreadgroup(group, "consumer", {stream: ">"})
        monkeypatch.setattr(
            processor,
            "_bulk_index",
            lambda _client, entries, *, processor_pod: {
                item_id: "mapping failure" for item_id, _fields, _duplicate in entries
            },
        )

        processor.process_entries(redis_client, object(), [(message_id, fields)])

        dead_letters = redis_client.xrange(dead_letter_stream)
        assert len(dead_letters) == 1
        assert dead_letters[0][1]["source_id"] == message_id
        assert dead_letters[0][1]["attempts"] == "1"
        assert redis_client.xpending(stream, group)["pending"] == 0
        assert redis_client.hget(processor.ATTEMPTS_KEY, message_id) is None
        assert redis_client.zscore(processor.ACKED_KEY, message_id) is not None
        assert redis_client.get(processor._dedup_key(log_id)) is None
    finally:
        redis_client.delete(stream, dead_letter_stream, processor._dedup_key(log_id))
        if message_id is not None:
            redis_client.hdel(processor.ATTEMPTS_KEY, message_id)
            redis_client.zrem(processor.ACKED_KEY, message_id)


@pytest.mark.integration
def test_stream_retention_removes_only_expired_acknowledged_entries(
    redis_service, monkeypatch
):
    redis_client = redis_service
    suffix = uuid.uuid4().hex
    stream = f"test:logs:{suffix}"
    group = f"test-group:{suffix}"
    monkeypatch.setattr(processor, "STREAM_NAME", stream)
    monkeypatch.setattr(processor, "GROUP_NAME", group)
    monkeypatch.setattr(processor, "STREAM_RETENTION_SECONDS", 60)
    message_ids = []

    try:
        redis_client.xgroup_create(stream, group, id="0", mkstream=True)
        expired_id = redis_client.xadd(stream, {"log_id": "expired"})
        recent_id = redis_client.xadd(stream, {"log_id": "recent"})
        pending_id = redis_client.xadd(stream, {"log_id": "pending"})
        message_ids.extend((expired_id, recent_id, pending_id))
        redis_client.xreadgroup(group, "consumer", {stream: ">"}, count=3)

        monkeypatch.setattr(processor.time, "time", lambda: 100.0)
        processor._ack_success(redis_client, expired_id)
        monkeypatch.setattr(processor.time, "time", lambda: 990.0)
        processor._ack_success(redis_client, recent_id)
        monkeypatch.setattr(processor.time, "time", lambda: 1000.0)

        assert processor._prune_acknowledged(redis_client) == 1
        assert [message_id for message_id, _fields in redis_client.xrange(stream)] == [
            recent_id,
            pending_id,
        ]
        assert redis_client.zscore(processor.ACKED_KEY, expired_id) is None
        assert redis_client.zscore(processor.ACKED_KEY, recent_id) == 990.0
        assert redis_client.xpending(stream, group)["pending"] == 1
    finally:
        redis_client.delete(stream)
        if message_ids:
            redis_client.zrem(processor.ACKED_KEY, *message_ids)
