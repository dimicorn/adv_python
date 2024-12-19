import asyncio
import sys
from typing import TypeVar
from asyncio import StreamReader, StreamWriter
from stem.envelope import Envelope
from multiprocessing import Process

T = TypeVar("T")


async def get_server(servers: list[tuple[str, int]]) -> tuple[str, int]:
    powerfullities = []
    for host, port in servers:
        reader_new, writer_new = await asyncio.open_connection(host, port)
        writer_new.write(Envelope(dict(command="powerfullity")).to_bytes())
        await writer_new.drain()
        response = await reader_new.read()
        envelope = Envelope.from_bytes(response)
        powerfullities.append([host, port, envelope.meta["powerfullity"]])
        writer_new.close()
        await writer_new.wait_closed()
    powerfullities.sort(key=lambda x: x[2])
    return powerfullities[-1][0], powerfullities[-1][1]


async def send_message(envelop: Envelope, server: tuple[str, int]) -> Envelope:
    reader_new, writer_new = await asyncio.open_connection(server[0], server[1])
    writer_new.write(envelop.to_bytes())
    await writer_new.drain()
    response = await reader_new.read()
    writer_new.close()
    await writer_new.wait_closed()
    envelope = Envelope.from_bytes(response)
    return envelope


class Distributor:
    server = None

    def __init__(self, servers):
        self.servers = servers

    async def __call__(self, reader: StreamReader, writer: StreamWriter):
        try:
            envelop_data = await Envelope.async_read(reader)
        except:
            writer.write(
                Envelope({'status': 'failed', 'error': 'Not Envelop format'}).to_bytes())
            await writer.drain()
            sys.exit('Not Envelop format')

        meta = envelop_data.meta
        data = envelop_data.data
        if 'command' in meta:
            match meta['command']:
                case 'run':
                    if 'task_path' in meta:
                        Distributor.server = await get_server(self.servers)
                        envelope = await send_message(
                            Envelope(
                                dict(command="run", task_path=meta['task_path']), data),
                            Distributor.server
                        )

                        task_result = envelope.meta['task_result']
                        writer.write(
                            Envelope({"status": "success", 'task_result': task_result}).to_bytes())
                case 'structure':
                    Distributor.server = await get_server(self.servers)
                    envelope = await send_message(Envelope(dict(command="structure")), Distributor.server)

                    dc = envelope.meta['structure']
                    dc['status'] = 'success'
                    writer.write(Envelope(dc).to_bytes())
                case 'powerfullity':
                    sm = 0
                    for server in self.servers:
                        envelope = await send_message(Envelope(dict(command="powerfullity")), (server[0], server[1]))
                        sm += envelope.meta["powerfullity"]
                    writer.write(
                        Envelope({"status": "success", 'powerfullity': sm}).to_bytes())
        else:
            writer.write(
                Envelope({'status': 'failed', 'error': 'KeyError: command'}).to_bytes())

        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def start_distributor(host: str, port: int, servers: list[tuple[str, int]]):
    server = await asyncio.start_server(Distributor(servers), host, port)
    async with server:
        await server.serve_forever()


def start_distributor_general(host: str, port: int, servers: list[tuple[str, int]]):
    asyncio.run(start_distributor(host, port, servers))


def start_distributor_in_subprocess(host: str, port: int, servers: list[tuple[str, int]]) -> Process:
    my_thread = Process(target=start_distributor_general,
                        args=(host, port, servers), daemon=True)
    my_thread.start()
    return my_thread
