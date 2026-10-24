import generator


def test_new_log_has_expected_fields_and_level():
    log = generator.new_log()

    assert {"log_id", "timestamp", "level", "service", "message", "trace_id"} <= log.keys()
    assert log["level"] in generator.LEVELS
    assert log["service"] in generator.SERVICES
    assert log["message"]
