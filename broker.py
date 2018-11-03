
# first of all import the socket library
import socket
import json
import datetime
import hashlib

# messages
message = {
    "action": "",
    "topic": "",
    "msg": ""
}

# list to hold subscription topics
# student topic is explicitly added
topics = [
    {"student": []}
]


def log_message(m):
    print(str(datetime.datetime.now()) + ": " + m)


def digest_message(m):
    h = hashlib.sha256()
    h.update(m.encode('ascii'))
    dig = h.digest()
    return hex(dig)


s = socket.socket()
port = 8080
s.bind(('', port))
s.listen(5)
log_message("broker is listening locally at " + str(port))

while True:
    conn, addr = s.accept()
    received = conn.recv(1024)
    d = received.decode("ascii")
    r = json.dumps(d)
    conn.close()
    msg = r["msg"]
    digest = digest_message(msg).encode('ascii')
    conn.sendall(digest)
    conn.close()

    # to subscribe the subscriber should send the topic
    # they want to subscribe to with a message with the
    # formateed as host:ports
    if r["action"] == "sub":
        topics[r["topic"]].append(r["msg"])
        print(addr)
    elif r["action"] == "pub":
        for sub in topics[r["topic"]]:
            a = sub.split(':')
            sub_host = a[0]
            sub_port = a[1]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((sub_host, sub_port))
                s.sendall(msg.encode('ascii'))
                rec = s.recv(1024)
                print(rec)
