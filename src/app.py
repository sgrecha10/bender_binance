import asyncio
import json
import time

import aiohttp
from aiohttp import web, ClientSession, WSCloseCode, http_exceptions
import logging
import socket
import weakref
# from src.kafka_client import KafkaProducerClient
from confluent_client import AIOProducer, Producer
from confluent_kafka import KafkaException


logger = logging.getLogger(__name__)
global producer, aio_producer

url = 'wss://stream.binance.com:9443/ws'
stream_name = 'btcusdt@depth@100ms'
topic_name = 'BTDUSDT'
# producer = KafkaProducerClient('anna')
config = {"bootstrap.servers": "kafka:9092"}


async def create_item1(message):
    if not isinstance(message, (bytes, str)):
        message = json.dumps(message).encode('utf-8')
    try:
        result = await aio_producer.produce(topic_name, message)
        return {"timestamp": result.timestamp()}
    except KafkaException as ex:
        raise http_exceptions.HttpProcessingError(
            # status_code=500, detail=ex.args[0].str()
        )

async def init_connect(app):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as ws:
            stream = [stream_name]
            json_msg = json.dumps({"method": "SUBSCRIBE", "params": stream, "id": int(time.time() * 1000)})
            await ws.send_str(json_msg)

            async for msg in ws:
                print(msg.data)

                await create_item1(msg.data)


                # await producer.message_handler(msg.data)
                # for client_ws in app['websockets']:
                #     if msg.type == aiohttp.WSMsgType.TEXT:
                #         await client_ws.send_str(msg.data)
                #     elif msg.type == aiohttp.WSMsgType.CLOSED:
                #         await client_ws.close()
                #         break
                #     elif msg.type == aiohttp.WSMsgType.ERROR:
                #         break


async def handle_request(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    request.app['websockets'].add(ws)
    await ws.send_str('websocket connected')

    try:
        async for msg in ws:
            await ws.send_str(msg)
    finally:
        request.app['websockets'].discard(ws)

    return ws


async def background_tasks(app):
    app['state'] = {
        'connect': asyncio.create_task(init_connect(app)),
    }
    yield
    app['state']['connect'].cancel()
    await app['state']['connect']


async def on_shutdown(app):
    for ws in set(app['websockets']):
        await ws.close(
            code=WSCloseCode.GOING_AWAY,
            message='Server shutdown',
        )
    print('Server shutdown')


async def client_stop(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    request.app['state']['connect'].cancel()

    return ws


async def client_start(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    request.app['state']['connect'] = asyncio.create_task(init_connect(request.app))

    return ws


async def startup_event(app):
    global producer, aio_producer
    aio_producer = AIOProducer(config)
    producer = Producer(config)


def shutdown_event():
    aio_producer.close()
    producer.close()


async def init_app():
    app = web.Application()
    app['websockets'] = weakref.WeakSet()

    app.add_routes([
        web.get('/', handle_request),
        web.get('/start', client_start),
        web.get('/stop', client_stop),
    ])

    app.cleanup_ctx.extend([
        background_tasks,
    ])

    # app.on_cleanup.extend([
    #     close_http_client,
    # ])

    app.on_startup.append(
        startup_event,
    )

    app.on_shutdown.extend([
        on_shutdown,
        shutdown_event,
    ])
    print('Server started')
    return app


if __name__ == '__main__':
    app = init_app()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('0.0.0.0', 8457))
    web.run_app(app, sock=sock)
