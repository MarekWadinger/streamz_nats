# NATS plugin for Streamz

This a plugin for [Streamz](https://github.com/python-streamz/streamz) that
adds stream nodes for writing and reading data from/to
[NATS](https://github.com/nats-io/nats.py).

## 🛠 Installation

Latest stable version is available on PyPI

```sh
pip install streamz_nats
```

Latest development version can be installed from git repo

```sh
pip install git+https://github.com/MarekWadinger/streamz_nats
```

## ⚡️ Quickstart

To start working with streamz_nats, follow these 3 steps:

### 1. Run a standalone NATS cluster locally

```sh
docker run -t -d -p 4222:4222 --name streamz-nats nats:latest
```

### 2. Create a consumer

The following example creates a consumer of the `greet.*` topics, where `*` is
wildcard for any substring, receives incoming messages, prints the content and
ID of messages that arrive, and acknowledges each message to the Pulsar broker.

```python
import nats
from streamz import Stream

s = Stream.from_nats(
    'nats://localhost:4222',
    ['greet.*'],
    )

s.map(lambda x: x.decode())
L = s.sink_to_list()

s.start()
while True:
    try:
        if L:
            print(L.pop(-1))
```
