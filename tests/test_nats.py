import asyncio
import atexit
import logging
from functools import partial
import os
import shlex
import subprocess
from time import time, sleep

import nats
import pytest
from nats.aio.msg import Msg
from nats.errors import TimeoutError

LAUNCH_NATS = os.environ.get('STREAMZ_LAUNCH_NATS', 'true') == 'true'

logger = logging.getLogger()
debug_handler = logging.FileHandler('test_debug.log')
debug_handler.setLevel(logging.DEBUG)


class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def wait_for(predicate, timeout, fail_func=None, period=0.001):
    """Wait for predicate to turn true, or fail this test"""
    # from distributed.utils_test
    deadline = time() + timeout
    while not predicate():
        sleep(period)
        if time() > deadline:  # pragma: no cover
            if fail_func is not None:
                fail_func()
            pytest.fail("condition not reached within %s seconds" % timeout)


def stop_docker(name='streamz-nats', cid=None, let_fail=False):
    """Stop docker container with given name tag

    Parameters
    ----------
    name: str
        name field which has been attached to the container we wish to remove
    cid: str
        container ID, if known
    let_fail: bool
        whether to raise an exception if the underlying commands return an
        error.
    """
    try:
        if cid is None:
            print('Finding %s ...' % name)
            cmd = shlex.split('docker ps -q --filter "name=%s"' % name)
            cid = subprocess.check_output(cmd).strip().decode()
        if cid:  # pragma: no cover
            print('Stopping %s ...' % cid)
            subprocess.call(['docker', 'rm', '-f', cid])
    except subprocess.CalledProcessError as e:  # pragma: no cover
        print(e)
        if not let_fail:
            raise


def launch_nats():
    stop_docker(let_fail=True)
    subprocess.call(shlex.split("docker pull nats:latest"))
    cmd = ("docker run -t -d -p 4222:4222 "
           "--name streamz-nats nats:latest -js")
    print(cmd)
    cid = subprocess.check_output(shlex.split(cmd)).decode()[:-1]

    def end():
        if cid:  # pragma: no cover
            stop_docker(cid=cid)
    atexit.register(end)

    def predicate():
        try:
            out = subprocess.check_output(
                ['docker', 'logs', cid],
                stderr=subprocess.STDOUT)
            return b'Server is ready' in out
        except subprocess.CalledProcessError:  # pragma: no cover
            pass
    wait_for(predicate, 50, period=0.1)
    return cid


def start_nats():
    print(color.BOLD, "Start Docker container with nats server", color.END)
    if LAUNCH_NATS:
        launch_nats()
    else:
        raise pytest.skip.Exception(  # pragma: no cover
            "nats not available. "
            "To launch nats use `export STREAMZ_LAUNCH_NATS=true`")


@pytest.fixture(scope="session", autouse=True)
def start_pytest_session():
    start_nats()


def print_test_name(test_name):
    test_name = (test_name
                 .replace("_", " ").upper().strip())
    print(color.BOLD, "\n", "="*3, test_name, "="*(76-len(test_name)),
          color.END)


async def _test_nats_vs_jetstream():
    print_test_name(_test_nats_vs_jetstream.__name__)
    try:
        nc = await nats.connect("nats://localhost:4222")
        # Create JetStream context.
        js = nc.jetstream()
        # Persist messages on subject.
        await js.add_stream(name="test-stream", subjects=["test0"])

        print("Publish message 0\n")
        await nc.publish("test0", b'0')
        print("Create subscriptions\n")

        async def cb_to_list(msg: Msg, L):
            L.append(msg.data.decode())
            await asyncio.sleep(0.5)
        L_nats = []
        await nc.subscribe(
            "test0", cb=partial(cb_to_list, L=L_nats))
        L_jet = []
        await js.subscribe(
            "test0", cb=partial(cb_to_list, L=L_jet))
        print("Publish messages 1 - 3\n")
        for i in range(1, 4):
            await nc.publish("test0", f'{i}'.encode())
        print(color.BOLD, "Collecting Results", color.END)
        await asyncio.sleep(3)
        print(f"NATS subscriber received {L_nats}")
        assert len(L_nats) == 3
        print(f"Jetstream subscriber received {L_jet}")
        assert len(L_jet) == 4

    finally:
        await js.delete_stream("test-stream")
        await nc.close()


def test_nats_vs_jetstream():
    asyncio.run(_test_nats_vs_jetstream())


async def _test_nats_slow_consumer():
    print_test_name(_test_nats_slow_consumer.__name__)
    import datetime as dt

    def now():
        return dt.datetime.utcnow().replace(microsecond=0)
    try:
        nc = await nats.connect("nats://localhost:4222")
        # Create JetStream context.
        js = nc.jetstream()
        # Persist messages on subject.
        await js.add_stream(name="test-stream", subjects=["test0"])
        print("Create subscriptions\n")

        async def cb_to_list(msg: Msg, L, sleep=False):
            L.append(f"R ({now()}): {msg.subject, msg.data.decode()}")
            if sleep:
                await asyncio.sleep(3)
        L_nats = []
        await js.subscribe(
            "test0", cb=partial(cb_to_list, L=L_nats))
        L_slow_1 = []
        await js.subscribe(
            "test0", cb=partial(cb_to_list, L=L_slow_1, sleep=True),
            pending_msgs_limit=1)
        L_slow = []
        await js.subscribe(
            "test0", cb=partial(cb_to_list, L=L_slow, sleep=True))

        print("Publishing 10 messages")
        for i in range(1, 10):
            await js.publish("test0", f'{i}'.encode())
            print(f"P ({now()}): {'test0', f'{i}'.encode()}")
            await asyncio.sleep(1)

        print(color.BOLD, "Collecting Results", color.END)
        await asyncio.sleep(10)
        print("Fast push subscriber received:")
        print(*L_nats, sep="\n")
        assert len(L_nats) == 9
        print("Slow push subscriber with single pending msg received:")
        print(*L_slow_1, sep="\n")
        assert len(L_slow_1) == 4
        print("Slow push subscriber with defaunt pending msg received:")
        print(*L_slow, sep="\n")
        assert len(L_slow) == 7
    finally:
        await js.delete_stream("test-stream")
        await nc.close()


def test_nats_slow_consumer():
    asyncio.run(_test_nats_slow_consumer())


async def _test_jetstream_push_vs_pull_subscribe():
    print_test_name(_test_jetstream_push_vs_pull_subscribe.__name__)
    try:
        nc = await nats.connect("nats://localhost:4222")
        # Create JetStream context.
        js = nc.jetstream()
        # Persist messages on subject.
        await js.add_stream(name="test-stream", subjects=["test.*"])
        print("Create push and durable pull subscriptions\n")
        sub = await js.subscribe("test.*")
        psub = await js.pull_subscribe("test.*", "psub")

        print("Publishing 5 messages to 5 subjects")
        await asyncio.sleep(0.1)
        # Publish messages to subjects sequentially
        for i in range(5):
            for j in range(5):
                await nc.publish(f"test.{i}", b'test.%d' % j)

        print(color.BOLD, "Collecting Results", color.END)
        # Check all the available messages
        for i in range(5):
            for j in range(5):
                msg = await sub.next_msg()
                assert msg.subject == f'test.{i}'
                assert msg.data.decode() == f'test.{j}'
        print("Push subscriber received all the messages")
        for i in range(5):
            for j in range(5):
                msgs = await psub.fetch(1)
                for msg in msgs:
                    assert msg.subject == f'test.{i}'
                    assert msg.data.decode() == f'test.{j}'
        print("Durable pull subscriber fetched all the messages")

        print("Unsubscribe all\n")
        await sub.unsubscribe()
        await psub.unsubscribe()

        print("Resubscribe subscriptions\n")
        sub = await js.subscribe("test.*")
        psub = await js.pull_subscribe("test.*", "psub")
        # Check all the available messages
        for i in range(5):
            for j in range(5):
                msg = await sub.next_msg()
                assert msg.subject == f'test.{i}'
                assert msg.data.decode() == f'test.{j}'
        print("Push subscriber received all the previous messages")
        try:
            msgs = await psub.fetch(1)
        except TimeoutError:
            assert True
        print("Durable pull subscriber fetched none of the previous messages")

    finally:
        await js.delete_stream("test-stream")
        await nc.close()


def test_jetstream_push_vs_pull_subscribe():
    asyncio.run(_test_jetstream_push_vs_pull_subscribe())


async def _test_jetstream_acknowledgements():
    print_test_name(_test_jetstream_acknowledgements.__name__)
    import random
    random.seed(0)
    try:
        nc = await nats.connect("nats://localhost:4222")
        # Create JetStream context.
        js = nc.jetstream()
        # Persist messages on subject.
        await js.add_stream(name="test-stream", subjects=["test.*"])

        print("Create subscriptions acknowledging all\n")

        async def cb_to_list(msg: Msg, L, nak_randomly=False):
            if nak_randomly and (random.randint(0, 1) == 1):
                await msg.nak()
            else:
                L.append(msg.data.decode())
                await msg.ack()
            return L

        L_push_cb = []
        sub_push_cb = await js.subscribe(
            "test.0", cb=partial(cb_to_list, L=L_push_cb))
        L_push = []
        sub_push = await js.subscribe(
            "test.0")
        L_pull = []
        sub_pull = await js.pull_subscribe(
            "test.0", durable="psub")

        print("Publishing 10 messages")
        await asyncio.sleep(0.1)
        # Publish messages to subjects sequentially
        for i in range(10):
            await nc.publish("test.0", f'{i}'.encode())
        await asyncio.sleep(3)

        while True:
            try:
                msg = await sub_push.next_msg()
                L_push = await cb_to_list(msg, L_push)
            except TimeoutError:
                break
        while True:
            try:
                msgs = await sub_pull.fetch(1)
                for msg in msgs:
                    L_pull = await cb_to_list(msg, L_pull)
            except TimeoutError:
                break

        print(color.BOLD, "Collecting Results", color.END)
        print(f"Async push subscriber received {L_push_cb}")
        print(f"Sync push subscriber received {L_push}")
        print(f"Pull subscriber received {L_pull}")

        await sub_push_cb.unsubscribe()
        await sub_push.unsubscribe()
        await sub_pull.unsubscribe()

        print("\nCreate subscriptions not acknowledging by random\n")
        L_push_cb = []
        sub_push_cb = await js.subscribe(
            "test.0", cb=partial(cb_to_list, L=L_push_cb, nak_randomly=True))
        L_push = []
        sub_push = await js.subscribe(
            "test.0")
        L_pull = []
        sub_pull = await js.pull_subscribe(
            "test.0", durable="psub2")

        while True:
            try:
                msg = await sub_push.next_msg()
                L_push = await cb_to_list(msg, L_push, nak_randomly=True)
            except TimeoutError:
                break
        while True:
            try:
                msgs = await sub_pull.fetch(1)
                for msg in msgs:
                    L_pull = await cb_to_list(msg, L_pull, nak_randomly=True)
            except TimeoutError:
                break

        print(color.BOLD, "Collecting Results", color.END)
        print(f"Async push subscriber received {L_push_cb}")
        print(f"Sync push subscriber received {L_push}")
        print(f"Pull subscriber received {L_pull}")

    finally:
        await js.delete_stream("test-stream")
        await nc.close()


def test_jetstream_acknowledgements():
    asyncio.run(_test_jetstream_acknowledgements())


async def _test_jetstream_delivery_group():
    print_test_name(_test_jetstream_delivery_group.__name__)
    try:
        nc = await nats.connect("nats://localhost:4222")
        # Create JetStream context.
        js = nc.jetstream()
        # Persist messages on subject.
        await js.add_stream(name="test-stream", subjects=["test.*"])

        print("Create two subscribes\n")

        async def cb_to_list(msg: Msg, L):
            L.append(msg.data.decode())
        L_w1 = []
        sub_w1 = await js.subscribe(
            "test.0", cb=partial(cb_to_list, L=L_w1))
        L_w2 = []
        sub_w2 = await js.subscribe(
            "test.0", cb=partial(cb_to_list, L=L_w2))

        print("Publishing 10 messages")
        await asyncio.sleep(0.1)
        # Publish messages to subjects sequentially
        for i in range(10):
            await nc.publish("test.0", f'{i}'.encode())
        await asyncio.sleep(1)

        await sub_w1.unsubscribe()
        await sub_w2.unsubscribe()

        print(color.BOLD, "Collecting Results", color.END)
        assert sorted(L_w1) == L_w1
        print(f"First subscriber in group received ordered messages: {L_w1}")
        assert sorted(L_w2) == L_w2
        print(f"Second subscriber in group received ordered messages: {L_w2}")
        assert len(L_w1) + len(L_w2) == 20
        print("All messages were received")

        print("Create delivery group\n")

        async def cb_to_list(msg: Msg, L):
            L.append(msg.data.decode())
        L_w1 = []
        sub_w1 = await js.subscribe(
            "test.1", cb=partial(cb_to_list, L=L_w1), queue="workers")
        L_w2 = []
        sub_w2 = await js.subscribe(
            "test.1", cb=partial(cb_to_list, L=L_w2), queue="workers")

        print("Publishing 10 messages")
        await asyncio.sleep(0.1)
        # Publish messages to subjects sequentially
        for i in range(10):
            await nc.publish("test.1", f'{i}'.encode())
        await asyncio.sleep(1)

        await sub_w1.unsubscribe()
        await sub_w2.unsubscribe()

        print(color.BOLD, "Collecting Results", color.END)
        print(f"First subscriber in group received ordered messages: {L_w1}")
        assert sorted(L_w1) == L_w1
        print(f"Second subscriber in group received ordered messages: {L_w2}")
        assert sorted(L_w2) == L_w2
        print("All messages were received")
        assert len(L_w1) + len(L_w2) == 10

        print("\nCreate delivery group with pickly consumers\n")
        print("Consumers apply back pressure by not acking some messages")

        async def cb_to_list_(msg: Msg, L, ack_rule):
            if ack_rule(msg.data.decode()):
                L.append(msg.data.decode())
                await msg.ack()
            else:
                await msg.nak()
        L_w1 = []
        ack_w1 = lambda x: int(x) % 2 == 0
        sub_w1 = await js.subscribe(
            "test.2",
            cb=partial(cb_to_list_, L=L_w1, ack_rule=ack_w1),
            queue="workers_2")
        L_w2 = []
        ack_w2 = lambda x: int(x) % 2 != 0
        sub_w2 = await js.subscribe(
            "test.2",
            cb=partial(cb_to_list_, L=L_w2, ack_rule=ack_w2),
            queue="workers_2")

        print("Publishing 10 messages")
        # Publish messages to subjects sequentially
        for i in range(10):
            await nc.publish("test.2", f'{i}'.encode())
        await asyncio.sleep(1)
        print(color.BOLD, "Collecting Results", color.END)
        print(f"Even subscriber in group received: {L_w1}")
        print(f"Odd subscriber in group received: {L_w2}")
        assert len(L_w1) + len(L_w2) == 10
        print("All messages were received")

        await sub_w1.unsubscribe()
        await sub_w2.unsubscribe()

    finally:
        await js.delete_stream("test-stream")
        await nc.close()


def test_jetstream_delivery_group():
    asyncio.run(_test_jetstream_delivery_group())


if __name__ == '__main__':
    start_nats()
    test_nats_vs_jetstream()
    test_nats_slow_consumer()
    test_jetstream_push_vs_pull_subscribe()
    test_jetstream_acknowledgements()
    test_jetstream_delivery_group()
