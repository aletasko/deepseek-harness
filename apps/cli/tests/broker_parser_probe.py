"""Feed JSON frames to the root broker's own parser and print a verdict per case.

Run as `python3 -I -B broker_parser_probe.py <broker checkout>` with a JSON list of {name, frame} on stdin.
The broker checkout is added to sys.path here because -I ignores the environment and the script directory.
Nothing is imported from the checkout except broker.protocol, which uses the standard library only.
"""
import json
import sys

sys.path.insert(0, sys.argv[1])
from broker import protocol  # noqa: E402


def verdict(frame):
    raw = json.dumps(frame, separators=(",", ":")).encode() + b"\n"
    kind = frame.get("type") if isinstance(frame, dict) else None
    try:
        if kind == "start":
            protocol.parse_start_frame(raw)
        elif kind == "register-core":
            protocol.parse_core_registration(raw)
        else:
            protocol.parse_binding_frame(raw)
    except protocol.ProtocolError as error:
        return {"accepted": False, "error": str(error)}
    return {"accepted": True}


cases = json.load(sys.stdin)
print(json.dumps([{"name": case["name"], **verdict(case["frame"])} for case in cases]))
