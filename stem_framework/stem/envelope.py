import array
import json
import mmap
import sys
from io import BytesIO
from asyncio import StreamReader, StreamWriter
from dataclasses import is_dataclass
from io import RawIOBase, BufferedReader
from json import JSONEncoder
from typing import Optional, Union, Any
from stem.meta import Meta

Binary = Union[bytes, bytearray, memoryview, array.array, mmap.mmap]


class MetaEncoder(JSONEncoder):
    def default(self, obj: Meta) -> Any:
        if is_dataclass(obj):
            return obj.__dict__
        else:
            return json.JSONEncoder.default(self, obj)


class Envelope:
    _MAX_SIZE = 128*1024*1024  # 128 Mb

    def __init__(self, meta: Meta, data: Optional[Binary] = b''):
        self.meta = meta
        if sys.getsizeof(data) >= self._MAX_SIZE:
            file_name = 'data.txt'
            with open('../data_files/{}'.format(file_name), 'wb') as f:
                f.write(data)
            with open('../data_files/{}'.format(file_name), 'rb') as f:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                    self.data = mmap_obj.read()
        else:
            self.data = data

    def __str__(self):
        return str(self.meta)

    @staticmethod
    def read(input: BufferedReader) -> "Envelope":
        input.read(2)
        type = input.read(4).decode("utf-8")
        meta_type = input.read(2).decode("utf-8")
        meta_length = int.from_bytes(input.read(4), byteorder='big')
        data_length = int.from_bytes(input.read(4), byteorder='big')
        input.read(4)
        meta = json.loads(input.read(meta_length).decode(
            "utf-8").replace("'", "\""))

        if data_length >= Envelope._MAX_SIZE:
            with mmap.mmap(input.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_obj:
                data = mmap_obj.read(data_length)
        else:
            data = input.read(data_length)
        return Envelope(meta, data)

    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        f = BytesIO(buffer)
        return Envelope.read(f)

    def to_bytes(self) -> bytes:
        type = b"DF02"
        meta_type = b"DI"
        meta = json.dumps(self.meta, cls=MetaEncoder)
        meta = str.encode(meta)
        meta_length = len(meta).to_bytes(4, byteorder='big')
        data_length = len(self.data).to_bytes(4, byteorder='big')
        byte_string = b'#~' + type + meta_type + meta_length + data_length + b'~#\r\n'
        byte_string += meta
        byte_string += self.data
        return byte_string

    def write_to(self, output: RawIOBase):
        output.write(self.to_bytes())

    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        await reader.read(2)
        type = (await reader.read(4)).decode("utf-8")
        meta_type = (await reader.read(2)).decode("utf-8")
        meta_length = int.from_bytes(await reader.read(4), byteorder='big')
        data_length = int.from_bytes(await reader.read(4), byteorder='big')
        await reader.read(4)
        meta = json.loads((await reader.read(meta_length)).decode("utf-8").replace("'", "\""))

        if data_length >= Envelope._MAX_SIZE:
            async with mmap.mmap(-1, length=0, access=mmap.ACCESS_READ) as mmap_obj:
                data = await mmap_obj.read(data_length)
        else:
            data = await reader.read(data_length)
        return Envelope(meta, data)

    async def async_write_to(self, writer: StreamWriter):
        writer.write(self.to_bytes())
