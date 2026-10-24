import os
import time
import socket
import json
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

import redis
from opensearchpy import OpenSearch, helpers

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "http://localhost:9200")
STREAM_NAME = os.environ.get("STREAM_NAME", "logs:raw")
GROUP_NAME = "logstream-cg"
CONSUMER_NAME = socket.gethostname()  # unique per pod replica
DEDUP_TTL_SECONDS = int(os.environ.get("DEDUP_TTL_SECONDS", "604800"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
PENDING_IDLE_MS = int(os.environ.get("PENDING_IDLE_MS", "5000"))
DLQ_STREAM_NAME = os.environ.get("DLQ_STREAM_NAME", "logs:dead")
STREAM_RETENTION_SECONDS = int(os.environ.get("STREAM_RETENTION_SECONDS", "604800"))
ATTEMPTS_KEY = "logs:attempts"
ACKED_KEY = "logs:acked"
_pending_cursor = "0-0"

COMPLETE_CLAIM_SCRIPT = """
local value = redis.call('GET', KEYS[1])
if value == ARGV[1] then
    redis.call('SET', KEYS[1], 'done:' .. ARGV[1], 'EX', ARGV[2])
    return 1
end
return 0
"""

DEAD_LETTER_SCRIPT = """
redis.call('XADD', KEYS[1], '*',
    'source_id', ARGV[1],
    'log_id', ARGV[2],
    'fields', ARGV[3],
    'attempts', ARGV[4],
    'error', ARGV[5],
    'failed_at', ARGV[6])
redis.call('XACK', KEYS[2], ARGV[7], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[5], ARGV[8], ARGV[1])
if redis.call('GET', KEYS[4]) == ARGV[1] then
    redis.call('DEL', KEYS[4])
end
return 1
"""


def connect_redis(url: str) -> redis.Redis:
    while True:
        try:
            r = redis.from_url(url, decode_responses=True)
            r.ping()
            print("Connected to Redis")
            return r
        except Exception as exc:
            print(f"Redis not ready: {exc} — retrying in 2s")
            time.sleep(2)


def connect_opensearch(url: str) -> OpenSearch:
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 9200
    http_auth = None
    if parsed.username and parsed.password:
        http_auth = (parsed.username, unquote(parsed.password))
    use_ssl = parsed.scheme == "https"
    client = OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=http_auth,
        http_compress=True,
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
    )
    while True:
        try:
            if client.ping():
                print("Connected to OpenSearch")
                return client
            else:
                print(f"OpenSearch ping returned False (auth failure?) — retrying in 2s")
        except Exception as exc:
            print(f"OpenSearch not ready: {exc} — retrying in 2s")
        time.sleep(2)


def _dedup_key(log_id: str) -> str:
    return f"logs:dedup:{log_id}"


def _claim_message(r: redis.Redis, log_id: str, message_id: str) -> str:
    """Return primary, duplicate, or defer using an atomic per-log claim."""
    key = _dedup_key(log_id)
    if r.set(key, message_id, nx=True, ex=DEDUP_TTL_SECONDS):
        return "primary"

    existing = r.get(key)
    if existing == message_id or existing == f"done:{message_id}":
        return "primary"
    if existing and existing.startswith("done:"):
        return "duplicate"
    return "defer"


def _complete_claim(r: redis.Redis, log_id: str, message_id: str) -> None:
    r.eval(COMPLETE_CLAIM_SCRIPT, 1, _dedup_key(log_id), message_id, DEDUP_TTL_SECONDS)


def _event_time(fields: dict) -> datetime:
    timestamp = fields.get("timestamp")
    if timestamp:
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _bulk_index(
    client: OpenSearch,
    entries: list[tuple[str, dict, bool]],
    *,
    processor_pod: str,
) -> dict[str, str | None]:
    """Return a result per stream ID; None means the document was indexed."""
    now = datetime.now(timezone.utc)
    actions = []
    for message_id, fields, is_duplicate in entries:
        event_time = _event_time(fields)
        actions.append({
            "_op_type": "index",
            "_index": f"logs-{event_time.strftime('%Y.%m.%d')}",
            "_id": message_id,
            "_source": {
                **fields,
                "@timestamp": event_time.isoformat(),
                "processed_at": now.isoformat(),
                "is_duplicate": is_duplicate,
                "processor_pod": processor_pod,
            },
        })

    results: dict[str, str | None] = {}
    for ok, item in helpers.streaming_bulk(
        client, actions, raise_on_error=False, raise_on_exception=False
    ):
        operation = next(iter(item.values()))
        message_id = operation.get("_id")
        results[message_id] = None if ok else str(operation.get("error", operation))
    return results


def _ack_success(r: redis.Redis, message_id: str) -> None:
    pipe = r.pipeline(transaction=True)
    pipe.xack(STREAM_NAME, GROUP_NAME, message_id)
    pipe.hdel(ATTEMPTS_KEY, message_id)
    pipe.zadd(ACKED_KEY, {message_id: time.time()})
    pipe.execute()


def _prune_acknowledged(r: redis.Redis) -> int:
    cutoff = time.time() - STREAM_RETENTION_SECONDS
    expired_ids = r.zrangebyscore(ACKED_KEY, min="-inf", max=cutoff)
    if not expired_ids:
        return 0
    pipe = r.pipeline(transaction=True)
    pipe.xdel(STREAM_NAME, *expired_ids)
    pipe.zrem(ACKED_KEY, *expired_ids)
    pipe.execute()
    return len(expired_ids)


def _record_failure(
    r: redis.Redis,
    message_id: str,
    fields: dict,
    error: str,
) -> None:
    attempts = r.hincrby(ATTEMPTS_KEY, message_id, 1)
    if attempts < MAX_RETRIES:
        print(f"Index failed for {message_id} (attempt {attempts}/{MAX_RETRIES}): {error}")
        return

    log_id = fields.get("log_id", "")
    r.eval(
        DEAD_LETTER_SCRIPT,
        5,
        DLQ_STREAM_NAME,
        STREAM_NAME,
        ATTEMPTS_KEY,
        _dedup_key(log_id) if log_id else "logs:dedup:missing",
        ACKED_KEY,
        message_id,
        log_id,
        json.dumps(fields, separators=(",", ":")),
        attempts,
        error[:2000],
        datetime.now(timezone.utc).isoformat(),
        GROUP_NAME,
        time.time(),
    )
    print(f"Moved {message_id} to {DLQ_STREAM_NAME} after {attempts} attempts")


def process_entries(
    r: redis.Redis,
    os_client: OpenSearch,
    entries: list[tuple[str, dict]],
    *,
    processor_pod: str = CONSUMER_NAME,
) -> None:
    candidates: list[tuple[str, dict, bool]] = []
    for message_id, fields in entries:
        log_id = fields.get("log_id")
        if not log_id:
            _record_failure(r, message_id, fields, "missing log_id")
            continue
        classification = _claim_message(r, log_id, message_id)
        if classification == "defer":
            continue
        candidates.append((message_id, fields, classification == "duplicate"))

    if not candidates:
        return

    try:
        results = _bulk_index(os_client, candidates, processor_pod=processor_pod)
    except Exception as exc:
        results = {message_id: str(exc) for message_id, _, _ in candidates}

    for message_id, fields, is_duplicate in candidates:
        error = results.get(message_id, "missing bulk result")
        if error is not None:
            _record_failure(r, message_id, fields, error)
            continue
        if not is_duplicate:
            _complete_claim(r, fields["log_id"], message_id)
        _ack_success(r, message_id)


def _recover_pending(r: redis.Redis) -> list[tuple[str, dict]]:
    global _pending_cursor
    _pending_cursor, entries, _ = r.xautoclaim(
        STREAM_NAME,
        GROUP_NAME,
        CONSUMER_NAME,
        min_idle_time=PENDING_IDLE_MS,
        start_id=_pending_cursor,
        count=100,
    )
    return entries


def main():
    r = connect_redis(REDIS_URL)
    os_client = connect_opensearch(OPENSEARCH_URL)

    try:
        r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"Consumer group '{GROUP_NAME}' created")
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        print(f"Consumer group '{GROUP_NAME}' already exists")

    while True:
        messages = r.xreadgroup(
            GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=100, block=1000
        )
        new_entries = messages[0][1] if messages else []
        pending_entries = _recover_pending(r)
        process_entries(r, os_client, pending_entries + new_entries)
        _prune_acknowledged(r)


if __name__ == "__main__":
    main()
