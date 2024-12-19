import socketserver
import types
from socketserver import StreamRequestHandler
from typing import Optional, TypeVar
from stem.envelope import Envelope
from stem.meta import Meta
from stem.remote.remote_workspace import RemoteTask
from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner
from stem.workspace import IWorkspace
from multiprocessing import Process

T = TypeVar("T")


def get_task_result(meta: Meta, data: tuple, task_remote: RemoteTask) -> list[T]:
    meta['workspace'] = UnitHandler.workspace
    meta['task_master'] = UnitHandler.task_master
    task_result_raw = task_remote.transform(meta).data
    task_result = []
    if isinstance(task_result_raw, types.GeneratorType):
        for k in task_result_raw:
            task_result.append(k)
    elif isinstance(task_result_raw, map):
        task_result = list(zip(data, task_result_raw))
    else:
        task_result.append(task_result_raw)
    return task_result


class UnitHandler(StreamRequestHandler):
    workspace: IWorkspace
    task_master = TaskMaster(SimpleRunner())
    powerfullity: int

    def handle(self):
        envelop_data = Envelope.read(self.rfile)
        meta = envelop_data.meta
        data = tuple(envelop_data.data)
        if 'command' in meta:
            match meta['command']:
                case 'run':
                    if 'task_path' in meta:
                        task = UnitHandler.workspace.find_task(
                            meta['task_path'])
                        task_remote = RemoteTask(self.request.getsockname()[0], self.request.getsockname()[1],
                                                 task.name)
                        task_result = get_task_result(meta, data, task_remote)
                        Envelope({"status": "success", "task_result": task_result}).write_to(
                            self.wfile)
                case 'structure':
                    dc = UnitHandler.workspace.structure()
                    dc["status"] = "success"
                    Envelope(dc).write_to(self.wfile)
                case 'powerfullity':
                    Envelope({"status": "success", 'powerfullity': UnitHandler.powerfullity}).write_to(
                        self.wfile)
                case 'stop':
                    self.server.shutdown()
                    self.server.server_close()
        else:
            Envelope({'status': 'failed', 'error': 'KeyError: command'}).write_to(
                self.wfile)


def start_unit(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None):
    UnitHandler.powerfullity = powerfullity
    UnitHandler.workspace = workspace
    with socketserver.TCPServer((host, port), UnitHandler) as server:
        server.serve_forever()


def start_unit_in_subprocess(workspace: IWorkspace, host: str, port: int,
                             powerfullity: Optional[int] = None) -> Process:
    my_thread = Process(target=start_unit, args=(
        workspace, host, port, powerfullity), daemon=True)
    my_thread.start()
    return my_thread
