"""
Create custom streamz sources.

Classes:

    from_nats
"""
import asyncio
import inspect
from typing import Callable, Union

import nats
from nats.aio.msg import Msg
from streamz import Source, Stream


@Stream.register_api(staticmethod)
class from_nats(Source):  # pylint: disable=C0103
    """ Accepts messages from nats

    Examples
    --------
    >>> import nats
    >>> from streamz import Stream
    >>> s = Stream.from_nats(
    ...     'nats://localhost:4222',
    ...     ['my-topic'],
    ...     )
    >>> decoder = s.map(lambda x: x.decode())
    >>> L = decoder.sink_to_list()
    """
    def __init__(
            self,
            service_url: Union[str, list[str]],
            topics: Union[str, list[str]],
            callback: Union[Callable, None] = None,
            poll_interval: float = 0.1,
            **kwargs):
        self.service_url = service_url
        if isinstance(topics, list):
            self.topics: str = '|'.join(topics)
        else:
            self.topics = topics
        if callback is None:
            callback = self._process_message
        self._cb = self._process_message
        self.poll_interval = poll_interval

        sig_stream = set(inspect.signature(Stream).parameters)
        sig_source = set(inspect.signature(Source).parameters)
        streamz_kwargs = {k: v for (k, v) in kwargs.items()
                          if (k in sig_stream) or (k in sig_source)}
        self.kwargs = {k: v for (k, v) in kwargs.items()
                       if (k not in sig_stream) and (k not in sig_source)}
        super().__init__(**streamz_kwargs)

    async def _process_message(self, message):
        self.emit(message.data.decode(), asynchronous=True)

    async def _run(self):
        # # Opt 1. With coroutine
        # #  Will not return any message until max_msgs is reached
        # while True:
        #     try:
        #         self.consumer = await self.client.subscribe(
        #               self.topics, max_msgs=5)
        #         tasks = [self._cb(msg)
        #                 async for msg in self.consumer.messages]
        #         # These three options seems to be equivalent
        #         # await asyncio.gather(*tasks, return_exceptions=True)
        #         # Should raise timeout error when all the tasks are not done
        #         await asyncio.wait_for(
        #             asyncio.gather(
        #                 *tasks, return_exceptions=True),
        #             timeout=self.poll_interval)
        #         # Should not raise timeout error and split the tasks
        #         await asyncio.wait(tasks, timeout=self.poll_interval)
        #         logging.info("done")
        #     except asyncio.TimeoutError:
        #         logging.info("timeout")
        #         break
        # # Opt 2.a Without coroutine in while loop
        # #  Will return messages if they arrive in time
        # self.sub = await self.client.subscribe(
        #                 self.topics,
        #                 cb=self._cb)
        # await asyncio.sleep(self.poll_interval)
        # # Will deliver remaining messages
        # await self.sub.drain()
        # # # Will not deliver remaining messages
        # # await self.sub.unsubscribe()
        # # logging.info("done")
        # Opt 3. Synchronous
        async for msg in self.sub.messages:
            await self._cb(msg)
            await asyncio.sleep(self.poll_interval)

    async def run(self):
        self.client = await nats.connect(self.service_url, **self.kwargs)
        # # Opt 2.b Without coroutine out of loop - will return all messages
        # self.sub = await self.client.subscribe(
        #                 self.topics,
        #                 cb=self._cb)
        # while not self.stopped:
        #     await asyncio.sleep(self.poll_interval)
        # # Opt 2.a Without coroutine in while loop
        # while not self.stopped:
        #     await self._run()
        # Opt 3. Synchronous
        self.sub = await self.client.subscribe(self.topics)
        while not self.stopped:
            await self._run()

    # TODO: drain client on stop
    # async def _stop(self):
    #     await self.sub.unsubscribe()
    #     await self.client.drain()

    # def stop(self):
    #     """set self.stopped, which will cause polling to stop after next run
    # """
    #     if not self.stopped:
    #         asyncio.run(self._stop())
    #         self.stopped = True


@Stream.register_api(staticmethod)
class from_jetstream(Source):  # pylint: disable=C0103
    """ Accepts messages from jetstream

    Examples
    --------
    >>> import nats
    >>> from streamz import Stream
    >>> s = Stream.from_jetstream(
    ...     'nats://localhost:4222',
    ...     ['my-topic'],
    ...     'test_subscription'
    ...     )
    >>> decoder = s.map(lambda x: x.decode())
    >>> L = decoder.sink_to_list()
    """
    def __init__(
            self,
            service_url: Union[str, list[str]],
            topics: Union[str, list[str]],
            subscription_name: str,
            callback: Union[Callable, None] = None,
            poll_interval: float = 0.1,
            **kwargs):
        self.service_url = service_url
        if isinstance(topics, list):
            self.topics: str = '|'.join(topics)
        else:
            self.topics = topics
        self.subscription_name = subscription_name
        if callback is None:
            callback = self._process_msg
        self._cb = self._process_msg
        self.poll_interval = poll_interval

        sig_stream = set(inspect.signature(Stream).parameters)
        sig_source = set(inspect.signature(Source).parameters)
        streamz_kwargs = {k: v for (k, v) in kwargs.items()
                          if (k in sig_stream) or (k in sig_source)}
        self.kwargs = {k: v for (k, v) in kwargs.items()
                       if (k not in sig_stream) and (k not in sig_source)}
        super().__init__(**streamz_kwargs)

    async def _process_msg(self, msg: Msg):
        try:
            self.emit(msg.data.decode(), asynchronous=True)
            await msg.ack()
        except Exception:  # pragma: no cover
            await msg.nak()

    async def _run(self):
        try:
            msgs = await self.sub.fetch(1)
            for msg in msgs:
                await self._cb(msg)
        except TimeoutError:  # pragma: no cover
            pass
        finally:
            await asyncio.sleep(self.poll_interval)

    async def run(self):
        self.nc = await nats.connect(self.service_url, **self.kwargs)
        self.client = self.nc.jetstream()
        self.sub = await self.client.pull_subscribe(
            self.topics, durable=self.subscription_name)
        while not self.stopped:
            await self._run()
