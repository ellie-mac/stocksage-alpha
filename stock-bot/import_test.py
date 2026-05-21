import sys
print("py ok", flush=True)
try:
    import lark_oapi
    print("lark_oapi ok", flush=True)
except Exception as e:
    print(f"lark_oapi FAIL: {e!r}", flush=True)
    raise
try:
    from lark_oapi.api.im.v1 import CreateMessageRequest
    print("im.v1 ok", flush=True)
except Exception as e:
    print(f"im.v1 FAIL: {e!r}", flush=True)
    raise
print("all imports OK", flush=True)
