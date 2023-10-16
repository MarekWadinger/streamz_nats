import asyncio
import atexit
import os
import shlex
import subprocess

import nats
import pytest
from streamz import Stream
from streamz.utils_test import wait_for

from streamz_nats.sources import from_nats  # noqa: F401

LAUNCH_NATS = os.environ.get('STREAMZ_LAUNCH_NATS', 'true') == 'true'


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
           "--name streamz-nats nats:latest")
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


async def _test_from_nats():
    print("starting")
    if LAUNCH_NATS:
        launch_nats()
    else:
        raise pytest.skip.Exception(  # pragma: no cover
            "nats not available. "
            "To launch nats use `export STREAMZ_LAUNCH_NATS=true`")

    nc = await nats.connect("nats://localhost:4222")
    stream = Stream.from_nats(  # type: ignore
        service_url="nats://localhost:4222",
        topics="test.*")
    out = stream.sink_to_list()
    stream.start()
    await asyncio.sleep(1.1)  # for loop to run
    for i in range(5):
        print('loop', i)
        await nc.publish(f"test.{i}", b'test.%d' % i)
        await asyncio.sleep(0.1)  # small pause ensures correct ordering
    # it takes some time for messages to come back out of nc
    print(out)
    wait_for(lambda: len(out) == 5, 5, period=0.1)
    assert out[-1] == 'test.4'


def test_from_nats():
    asyncio.run(_test_from_nats())
