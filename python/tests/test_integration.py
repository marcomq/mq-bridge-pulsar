"""End-to-end tests for the Pulsar endpoint as Python loads it: a native plugin.

This is the `cdylib` path — `register()` hands the bundled library to
mq-bridge's generic loader — so it exercises the plugin ABI, not the directly
linked factory that `cargo test --test integration` covers. Both must agree.

Run against the broker the Rust tests use:

    docker compose -f tests/docker-compose.yml up -d
    pip install mq-bridge-py mq-bridge-pulsar     # or a locally built wheel
    pytest python/tests -v
    docker compose -f tests/docker-compose.yml down

Every test skips (rather than fails) when the packages are missing or no broker
is listening, so the file is safe to collect in an environment without either.
"""

import json
import socket
import uuid

import pytest

PULSAR_URL = "pulsar://localhost:6650"
BROKER_HOST, BROKER_PORT = "localhost", 6650
ROWS = 100

mq_bridge = pytest.importorskip("mq_bridge", reason="mq-bridge-py is not installed")
mq_bridge_pulsar = pytest.importorskip(
    "mq_bridge_pulsar", reason="mq-bridge-pulsar is not installed"
)


def _broker_is_up() -> bool:
    try:
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=2):
            return True
    except OSError:
        return False


requires_broker = pytest.mark.skipif(
    not _broker_is_up(),
    reason=f"no Pulsar broker on {BROKER_HOST}:{BROKER_PORT} "
    "(docker compose -f tests/docker-compose.yml up -d)",
)


@pytest.fixture(scope="session", autouse=True)
def registered():
    """Registration is process-global, so do it once for the whole session."""
    assert mq_bridge_pulsar.register() == "pulsar"
    return "pulsar"


def _supports_initial_position() -> bool:
    """Whether the *installed* plugin knows the field.

    A wheel is a compiled artifact, so `pip install mq-bridge-pulsar` can easily
    be older than this checkout. Probing beats a confusing `unknown field`
    failure that looks like a bug in the endpoint rather than a stale install.
    """
    try:
        mq_bridge.Route.from_str(
            f"""
input:
{_pulsar_endpoint("persistent://public/default/probe", initial_position="earliest")}
output:
  file: {{ path: "/dev/null" }}
exit_on_empty: true
"""
        ).run()
    except Exception as error:  # noqa: BLE001 - any failure is inspected below
        return "unknown field `initial_position`" not in str(error)
    return True


requires_initial_position = pytest.mark.skipif(
    _broker_is_up() and not _supports_initial_position(),
    reason="the installed mq-bridge-pulsar predates `initial_position`; rebuild the "
    "wheel from this checkout: python -m mq_bridge.plugin_packaging "
    "--package python/mq_bridge_pulsar --out python/dist",
)


@pytest.fixture
def topic() -> str:
    """A fresh topic per test, so no test can see another's backlog."""
    return f"persistent://public/default/pytest-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def source_file(tmp_path):
    path = tmp_path / "in.jsonl"
    with path.open("w") as handle:
        for i in range(1, ROWS + 1):
            handle.write(json.dumps({"id": i, "name": f"order-{i}"}) + "\n")
    return path


def _pulsar_endpoint(topic: str, **extra) -> str:
    """The `custom` form is how every non-Rust host addresses a plugin."""
    config = {"url": PULSAR_URL, "topic": topic, **extra}
    lines = "\n".join(f"        {k}: {json.dumps(v)}" for k, v in config.items())
    return f"    custom:\n      name: pulsar\n      config:\n{lines}"


def _publish(source_file, topic: str) -> None:
    mq_bridge.Route.from_str(
        f"""
input:
  file: {{ path: "{source_file}", format: raw }}
output:
{_pulsar_endpoint(topic)}
exit_on_empty: true
"""
    ).run()


def _drain_to(out_path, topic: str, **extra) -> int:
    mq_bridge.Route.from_str(
        f"""
input:
{_pulsar_endpoint(topic, **extra)}
output:
  file: {{ path: "{out_path}", format: json }}
exit_on_empty: true
"""
    ).run()
    if not out_path.exists():
        return 0
    with out_path.open() as handle:
        return sum(1 for _ in handle)


def test_register_is_idempotent(registered):
    """Calling it again is a no-op, not the 'already registered' error."""
    assert mq_bridge_pulsar.register() == "pulsar"


def test_library_path_points_at_a_real_file():
    from pathlib import Path

    assert Path(mq_bridge_pulsar.library_path()).is_file()


@requires_broker
@requires_initial_position
def test_reads_a_backlog_published_before_the_subscription_existed(source_file, tmp_path, topic):
    """Regression: a Pulsar subscription is created at `latest`.

    Without `initial_position: earliest` the rows published below are invisible
    forever — the topic reports them under `msgInCounter` while the subscription
    shows `msgBacklog: 0`. This test publishes *first*, on purpose.
    """
    _publish(source_file, topic)
    out = tmp_path / "out.jsonl"

    assert _drain_to(out, topic, subscription="backlog-sub", initial_position="earliest") == ROWS

    first = json.loads(out.read_text().splitlines()[0])
    assert first["payload"] == {"id": 1, "name": "order-1"}


@requires_broker
def test_latest_is_the_default_and_does_not_see_the_backlog(source_file, tmp_path, topic):
    """The other half of the contract, so a future default flip is caught here."""
    _publish(source_file, topic)
    out = tmp_path / "out.jsonl"

    assert _drain_to(out, topic, subscription="latest-sub") == 0


@requires_broker
def test_round_trip_preserves_every_payload(source_file, tmp_path, topic):
    # Create the subscription before publishing, so `latest` still sees the rows.
    out = tmp_path / "out.jsonl"
    _drain_to(out, topic, subscription="rt-sub")
    _publish(source_file, topic)

    assert _drain_to(out, topic, subscription="rt-sub") == ROWS
    ids = [json.loads(line)["payload"]["id"] for line in out.read_text().splitlines()]
    assert sorted(ids) == list(range(1, ROWS + 1))


@requires_broker
def test_a_rejected_config_surfaces_as_an_error(tmp_path, topic):
    """A config the endpoint rejects must reach the caller, not hang.

    Scope: this proves the error *surfaces*. It does not prove the ABI status
    was classified as permanent, because `run()` on a drain route also raises
    via the startup timeout when the failure is merely retryable — so this test
    passes either way. The classification itself is asserted where it is
    observable: `plugin::endpoint` unit tests in the mq-bridge repo, and the
    directly linked path in `tests/integration.rs`.
    """
    route = mq_bridge.Route.from_str(
        f"""
input:
{_pulsar_endpoint(topic, definitely_not_a_field="x")}
output:
  file: {{ path: "{tmp_path / 'never.jsonl'}" }}
exit_on_empty: true
"""
    )
    with pytest.raises(Exception, match="unknown field|invalid Pulsar endpoint configuration"):
        route.run()
