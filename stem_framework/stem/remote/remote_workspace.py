from typing import Any, TypeVar

from stem.envelope import Envelope
from stem.meta import Meta, get_meta_attr
from stem.task import Task
from stem.workspace import IWorkspace

T = TypeVar("T")


class RemoteTask(Task):
    def __init__(self, address="localhost", port=9800, task_path: str = ''):
        self.address = address
        self.port = port
        if task_path == '':
            raise Exception("Incorrect task_path")
        self.task_path = task_path

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        task = get_meta_attr(meta, 'workspace').find_task(meta['task_path'])
        task_master = get_meta_attr(meta, 'task_master')
        task_meta = get_meta_attr(meta, 'task_meta', {})
        task_result_raw = task_master.execute(task_meta, task)
        return task_result_raw


class RemoteWorkspace(IWorkspace):

    def __init__(self, workspace: IWorkspace, address="localhost", port=9800):
        self.address = address
        self.port = port
        self.workspace = workspace

    @property
    def tasks(self) -> dict[str, Task]:
        return {t: RemoteTask(self.address, self.port, t) for t in self.workspace.tasks}

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return set([RemoteWorkspace(w, self.address, self.port) for w in self.workspaces])
