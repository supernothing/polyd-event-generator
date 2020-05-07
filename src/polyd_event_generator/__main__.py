import asyncio
import json
import logging
import queue
import signal
import threading
import sys
import os

import click
import websockets
from walrus import Database

from polyd_events import events, producer, event_types


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AsyncThread(threading.Thread):
    async def main_loop(self):
        raise NotImplemented()

    def run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.get_event_loop().run_until_complete(self.main_loop())


class WSThread(AsyncThread):
    def __init__(self, community, uri, q, retry=False):
        super().__init__()
        self.community = community
        self.uri = uri
        self.q = q
        self.stop_thread = False
        self.retry = retry

    async def main_loop(self):
        print('starting loop')
        while True:
            try:
                print('trying man')
                async with websockets.connect(self.uri) as websocket:
                    print('connected')
                    while not (websocket.closed or self.stop_thread):
                        try:
                            self.q.put(events.Event.from_event(self.community, json.loads(await
                                   asyncio.wait_for(websocket.recv(), 1))))
                        except asyncio.TimeoutError:
                            if self.stop_thread:
                                break
                            continue
                        except Exception as e:
                            logger.exception(e)
                            break
            except Exception as e:
                logger.exception(e)

            if self.stop_thread:
                break

            if not self.retry:
                logger.warning('Socket disconnected, exiting')
                # for now, stop everything if we don't retry
                os.kill(os.getpid(), signal.SIGINT)

            logger.warning('Socket disconnected, retrying in 1s...')
            await asyncio.sleep(1)

        self.q.put(None)

polyd_communities = {
    'nu': 'wss://nu.k.polyswarm.network/v1/events/?chain=side',
    'omicron': 'wss://omicron.k.polyswarm.network/v1/events/?chain=side',
}

@click.command()
@click.option('-e', '--event', multiple=True, type=click.Choice(event_types+['all']), default=['all'])
@click.option('--community', '-c', multiple=True, type=click.Choice(list(polyd_communities.keys())+['all']), default=['all'])
@click.option('--redis', '-h', type=click.STRING, envvar='POLYDMON_REDIS', default='',
              help='redis host backend')
@click.option('--retry', '-r', is_flag=True, default=False)
@click.option('--quiet', '-q', is_flag=True, default=False)
def event_generator(event, community, redis, retry, quiet):
    db = Database(redis)
    communities = community if 'all' not in community else polyd_communities.keys()

    ws_q = queue.Queue(maxsize=1000)
    ws_threads = [WSThread(c, polyd_communities[c], ws_q, retry) for c in communities]

    # lazily create producers
    producers = {
        f'polyd-{c}-all': producer.EventProducer(f'polyd-{c}-all', db, max_len=20000) for c in communities
    }

    for t in ws_threads:
        t.start()

    events = event

    def handler(_, __):
        for t in ws_threads:
            t.stop_thread = True

    signal.signal(signal.SIGINT, handler)

    # this will quit if either thread exits
    for event in iter(ws_q.get, None):
        print(event)
        if event.event in events or 'all' in events:
            stream_name = f'polyd-{event.community}-{event.event}'
            community_stream = f'polyd-{event.community}-all'
            if stream_name not in producers:
                producers[stream_name] = producer.EventProducer(stream_name, db, max_len=20000)
            producers[stream_name].add_event(event)
            producers[community_stream].add_event(event)

            if not quiet:
                logger.info(str(event))

    for t in ws_threads:
        t.join()
