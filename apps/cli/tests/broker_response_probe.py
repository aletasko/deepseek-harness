"""Print the frames the root broker's own daemon code builds for the frames Core sent.

Run as `python3 -I -B broker_response_probe.py <broker checkout>` with a JSON object {name: frame} on stdin. Every
answer is produced by the daemon: the Core frames go through the daemon's live control loop (`_control_thread`, a real
`_Session`, `CoreOwner` and kernel peer credentials over a socketpair) against a stub service that returns real broker
dataclasses, so `registered` is the daemon's own frame and every answer is encoded by the daemon (`version` included).
Refusals use `_dispatch` and `_error`; `worker-exited` is built by the broker's own `BrokerService._worker_exited`.
`out["live"]` says whether the live loop was available (it needs an unprivileged uid and the S6 daemon). Only the
standard library is used.
"""
import inspect
import json
import os
import socket
import sys
import threading
from types import SimpleNamespace

sys.path.insert(0, sys.argv[1])
from broker import daemon  # noqa: E402
from broker.assignments import Lease, RevokeAck  # noqa: E402
from broker.service import BrokerService, Refused  # noqa: E402

frames = json.load(sys.stdin)


class Stub:
    registry = SimpleNamespace(lease_ttl_ms=5000)

    def __init__(self, refuse=None):
        self.refuse = refuse

    def _maybe(self):
        return self.refuse

    def bind(self, core, epoch, frame):
        return self.refuse or SimpleNamespace(binding_id="b0ffee00" * 4, revision=3)

    def lease(self, core, epoch, frame):
        return self.refuse or Lease("Zm9vYmFy_-1", frame.binding_id, frame.revision, frame.mode, 0.0)

    def revoke(self, core, epoch, frame):
        return self.refuse or RevokeAck(frame.binding_id, (frame.binding_id, "child-1"))

    def start(self, core, epoch, frame):
        return self.refuse or {"pid": 4242, "workerId": "a1b2c3d4e5f6"}

    def signal(self, core, epoch, frame):
        return self.refuse or True

    def status(self):
        return {"bindings": 2, "leases": 1, "undrained": 0, "epoch": 7,
                "workers": [{"binding": "b0ffee", "workerId": "a1b2c3d4e5f6", "pid": 4242, "exited": False, "populated": True}]}


def encoded(response):
    """The frame as the daemon writes it (adds `version`), when the daemon has `_encode`; older brokers wrote it as is."""
    if hasattr(daemon, "_encode"):
        return json.loads(daemon._encode(response))
    return json.loads(json.dumps(response))


def respond(frame, refuse=None):
    raw = json.dumps(frame, separators=(",", ":")).encode() + b"\n"
    response, close = daemon._dispatch(Stub(refuse), None, 7, raw)
    return encoded(response)


class LiveStub(Stub):
    """What the live control loop needs from a service besides the frame handlers."""

    def __init__(self):
        super().__init__()
        self.registry = SimpleNamespace(lease_ttl_ms=5000, observe_core=lambda identity, revision: None)

    def events(self, *args, **kwargs):
        pass

    def attach_control(self, sink, drop):
        pass

    def detach_control(self, sink):
        pass


class AllowAll:
    def verify(self, _pinned):
        pass


def live_exchange(core_frames):
    """Send the Core frames through the daemon's live control loop; return the decoded answers by name."""
    from broker.core_owner import CoreOwner  # noqa: E402

    server_end, client_end = socket.socketpair()
    owner = CoreOwner(os.getuid())
    thread = threading.Thread(
        target=daemon._control_thread,
        args=(server_end, os.getuid(), owner, AllowAll(), LiveStub(), daemon._ControlSlot(), threading.Event()),
        daemon=True)
    thread.start()
    client_end.settimeout(10)
    reader = client_end.makefile("rb")
    answers = {}
    try:
        for name, frame in core_frames.items():
            client_end.sendall(json.dumps(frame, separators=(",", ":")).encode() + b"\n")
            line = reader.readline()
            answers[name] = json.loads(line) if line else {"closed": True}
    finally:
        reader.close()
        client_end.close()
        thread.join(15)
        owner.close()
    return answers


out = {}
for name, frame in frames.items():
    if frame["type"] != "register-core":
        try:
            out[name] = respond(frame)
        except Exception as error:  # the broker's parser refused the frame; the test reads only what it needs
            out[name] = {"refused-by-parser": type(error).__name__}
out["live"] = False
if os.getuid() != 0 and hasattr(daemon, "_ControlSlot"):
    try:
        ordered = {"register-core": frames["register-core"], **{name: frame for name, frame in frames.items() if name != "register-core"}}
        live = live_exchange(ordered)
        out["live"] = True
        out["registered"] = live["register-core"]
        for name in frames:
            if name != "register-core":
                out[name] = live[name]
    except Exception as error:  # the daemon predates the live loop this probe drives
        out["live-error"] = type(error).__name__
out["bind refused"] = respond(frames["bind room-seat"], Refused("bind-refused", "cwd-mismatch"))
out["start refused"] = respond(frames["start"], Refused("start-refused", "stdio-unsupported"))
out["revoke unconfirmed"] = respond(frames["revoke"], Refused("revoke-unconfirmed", "timeout"))
if "signal" in frames:
    try:
        out["signal refused"] = respond(frames["signal"], Refused("not-found", "unknown-worker"))
    except Exception as error:  # a broker from before contract point 11 does not parse `signal`
        out["signal refused"] = {"refused-by-parser": type(error).__name__}
def error(code, reason, request_id):
    """`_error(code, reason, request_id)` since the S6b reasons; `_error(code, request_id, reason)` before."""
    if list(inspect.signature(daemon._error).parameters)[1] == "reason":
        return daemon._error(code, reason, request_id)
    return daemon._error(code, request_id, reason) if reason is not None else daemon._error(code, request_id)


out["busy"] = encoded(error("busy", "too-many-in-flight", frames["status"]["requestId"]))
out["control-active"] = encoded(error("control-active", "another-control-connection", frames["status"]["requestId"]))
out["peer denied"] = encoded(dict(daemon._PEER_DENIED))
out["invalid frame"] = encoded(dict(daemon._INVALID_FRAME))

if hasattr(BrokerService, "_worker_exited"):  # absent from a broker that predates S6 contract point 7
    exits = []
    fake = SimpleNamespace(_control_lock=threading.Lock(), _exit_sink=exits.append)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", 0, None)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", None, 9)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", None, 65)
    out["worker-exited code"], out["worker-exited SIGKILL"], out["worker-exited SIG65"] = [encoded(frame) for frame in exits]
print(json.dumps(out))
