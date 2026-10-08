#!/usr/bin/env python3
"""A TCP listener that completes the handshake and then says nothing, ever.

This is the failure the testing team reported: an SFTP host that accepts the connection and then
stops responding. jsch blocks reading the SSH banner, and without a session timeout it blocks for
good - taking the inbound's polling thread with it.
"""
import socket, sys, threading

port = int(sys.argv[1]) if len(sys.argv) > 1 else 2022
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(16)
print(f"black hole listening on 127.0.0.1:{port}", flush=True)

held = []   # keep sockets open: closing would give the client an EOF to react to
def accept_forever():
    while True:
        try:
            conn, addr = srv.accept()
        except OSError:
            return
        held.append(conn)
        print(f"accepted {addr} - sending nothing", flush=True)

threading.Thread(target=accept_forever, daemon=True).start()
threading.Event().wait()
