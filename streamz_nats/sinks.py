"""
Create custom streamz sinks.

Classes:

    to_nats
"""
import asyncio
import inspect
from typing import Union

import nats
from streamz import Sink, Stream


@Stream.register_api()
class to_nats(Sink):  # pylint: disable=C0103
    """ Writes data in the stream to NATS

    Parameters
    ----------
    topic : string
        The topic which to write

    Examples
    --------
    # >>> from streamz import Stream
    # >>> source = Stream()
    # >>> producer = source.to_nats(
    # ...     'nats://localhost:4222'
    # ...     'my-response'
    # ...     )  # doctest: +SKIP
    # >>> for i in range(3):
    # ...     source.emit(('hello-nats-%d' % i).encode('utf-8'))
    """
    def __init__(
            self,
            upstream,
            service_url: Union[str, list[str]],
            topic: str,
            poll_interval: float = 0.1,
            **kwargs):
        self.service_url = service_url

        self.topic = topic
        self.poll_interval = poll_interval

        # take the stream specific kwargs out
        sig_stream = set(inspect.signature(Stream).parameters)
        sig_sink = set(inspect.signature(Sink).parameters)
        streamz_kwargs = {k: v for (k, v) in kwargs.items()
                          if (k in sig_stream) or (k in sig_sink)}
        self.kwargs = {k: v for (k, v) in kwargs.items()
                       if (k not in sig_stream) and (k not in sig_sink)}
        streamz_kwargs["ensure_io_loop"] = True
        super().__init__(upstream, **streamz_kwargs)

        self.stopped = False
        self.futures = []

    async def update(self, x: bytes, who=None, metadata=None):
        self.client = await nats.connect(self.service_url, **self.kwargs)
        await self.client.publish(self.topic, x)
        await asyncio.sleep(self.poll_interval)


@Stream.register_api()
class to_jetstream(Sink):  # pylint: disable=C0103
    """ Writes data in the stream to NATS Jetstream

    Parameters
    ----------
    topic : string
        The topic which to write

    Examples
    --------
    # >>> from streamz import Stream
    # >>> source = Stream()
    # >>> producer = source.to_jetstream(
    # ...     'nats://localhost:4222'
    # ...     'my-response',
    # ...     'test_producer'
    # ...     )  # doctest: +SKIP
    # >>> for i in range(3):
    # ...     source.emit(('hello-nats-%d' % i).encode('utf-8'))
    """
    def __init__(
            self,
            upstream,
            service_url: Union[str, list[str]],
            topic: str,
            stream_name: str,
            poll_interval: float = 0.1,
            **kwargs):
        self.service_url = service_url

        self.topic = topic
        self.stream_name = stream_name
        self.poll_interval = poll_interval

        # take the stream specific kwargs out
        sig_stream = set(inspect.signature(Stream).parameters)
        sig_sink = set(inspect.signature(Sink).parameters)
        streamz_kwargs = {k: v for (k, v) in kwargs.items()
                          if (k in sig_stream) or (k in sig_sink)}
        self.kwargs = {k: v for (k, v) in kwargs.items()
                       if (k not in sig_stream) and (k not in sig_sink)}
        streamz_kwargs["ensure_io_loop"] = True
        super().__init__(upstream, **streamz_kwargs)

        self.stopped = False
        self.futures = []

    async def update(self, x: bytes, who=None, metadata=None):
        self.nc = await nats.connect(self.service_url, **self.kwargs)
        self.client = self.nc.jetstream()
        await self.client.add_stream(
            name=self.stream_name, subjects=[self.topic])
        await self.client.publish(self.topic, x)
        await asyncio.sleep(self.poll_interval)
