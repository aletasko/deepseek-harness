"""Print the frames the root broker's own daemon code builds for the frames Core sent.

Run as `python3 -I -B broker_response_probe.py <broker checkout>` with a JSON object {name: frame} on stdin. The
daemon's `_dispatch` and `_error` are called with a stub service that returns real broker dataclasses; the
`worker-exited` frame is built by the broker's own `BrokerService._worker_exited`. Only the standard library is used.
"""
import json
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

    def status(self):
        return {"bindings": 2, "leases": 1, "undrained": 0, "epoch": 7,
                "workers": [{"binding": "b0ffee", "workerId": "a1b2c3d4e5f6", "pid": 4242, "exited": False, "populated": True}]}


def respond(frame, refuse=None):
    raw = json.dumps(frame, separators=(",", ":")).encode() + b"\n"
    response, close = daemon._dispatch(Stub(refuse), None, 7, raw)
    return response


out = {}
for name, frame in frames.items():
    if frame["type"] != "register-core":
        try:
            out[name] = respond(frame)
        except Exception as error:  # the broker's parser refused the frame; the test reads only what it needs
            out[name] = {"refused-by-parser": type(error).__name__}
out["bind refused"] = respond(frames["bind room-seat"], Refused("bind-refused", "cwd-mismatch"))
out["start refused"] = respond(frames["start"], Refused("start-refused", "stdio-unsupported"))
out["revoke unconfirmed"] = respond(frames["revoke"], Refused("revoke-unconfirmed", "timeout"))
out["busy"] = daemon._error("busy", frames["status"]["requestId"], "too-many-in-flight")
out["control-active"] = daemon._error("control-active", frames["status"]["requestId"])
out["peer denied"] = dict(daemon._PEER_DENIED)
out["invalid frame"] = dict(daemon._INVALID_FRAME)
out["registered"] = {"type": "registered", "requestId": frames["register-core"]["requestId"], "revision": 7}

if hasattr(BrokerService, "_worker_exited"):  # absent from a broker that predates S6 contract point 7
    exits = []
    fake = SimpleNamespace(_control_lock=threading.Lock(), _exit_sink=exits.append)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", 0, None)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", None, 9)
    BrokerService._worker_exited(fake, "b0ffee00" * 4, "a1b2c3d4e5f6", None, 65)
    out["worker-exited code"], out["worker-exited SIGKILL"], out["worker-exited SIG65"] = exits
print(json.dumps(out))
