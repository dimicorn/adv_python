"""
Conception modularity software
"""
from abc import ABC, abstractmethod, ABCMeta
from types import ModuleType
from typing import Optional, Any, Type, TypeVar, Union
from importlib import import_module

from stem.core import Named
from stem.meta import Meta
from stem.task import Task

T = TypeVar("T")


class TaskPath:
    def __init__(self, path: Union[str, list[str]]):
        if isinstance(path, str):
            self._path = path.split(".")
        else:
            self._path = path

    @property
    def is_leaf(self):
        return len(self._path) == 1

    @property
    def sub_path(self):
        return TaskPath(self._path[1:])

    @property
    def head(self):
        return self._path[0]

    @property
    def name(self):
        return self._path[-1]

    def __str__(self):
        return ".".join(self._path)


class ProxyTask(Task[T]):

    def __init__(self, proxy_name, task: Task):
        self._name = proxy_name
        self._task = task

    @property
    def dependencies(self):
        return self._task.dependencies

    @property
    def specification(self):
        return self._task.specification

    def check_by_meta(self, meta: Meta):
        self._task.check_by_meta(meta)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._task.transform(meta, **kwargs)


class IWorkspace(ABC, Named):
    @property
    @abstractmethod
    def tasks(self) -> dict[str, Task]:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> set["IWorkspace"]:
        pass

    def __eq__(self, obj: "IWorkspace"):
        return self.tasks == obj.tasks and self.workspaces == obj.workspaces

    def find_task(self, task_path: Union[str, TaskPath]) -> Optional[Task]:
        if not isinstance(task_path, TaskPath):
            task_path = TaskPath(task_path)
        if task_path.is_leaf:
            if task_path.name in self.tasks:
                for task in self.tasks:
                    if task == task_path.name:
                        return self.tasks[task]
            for wrs in self.workspaces:
                task = wrs.find_task(task_path)
                if task is not None:
                    return task
            return None
        else:
            for wks in self.workspaces:
                if wks.name == task_path.head:
                    return wks.find_task(task_path.sub_path)
            return None

    def has_task(self, task_path: Union[str, TaskPath]) -> bool:
        return IWorkspace.find_task(self, task_path) is not None

    def get_workspace(self, name) -> Optional["IWorkspace"]:
        for workspace in self.workspaces:
            if workspace.name == name:
                return workspace
            wrs = workspace.get_workspace(name)
            if wrs is not None:
                return wrs
        return None

    def structure(self) -> dict:
        return {
            "name": self.name,
            "tasks": list(self.tasks.keys()),
            "workspaces": [w.structure() for w in self.workspaces]
        }

    @staticmethod
    def find_default_workspace(task: Task) -> Type["IWorkspace"]:
        if hasattr(task, "_stem_workspace"):
            return getattr(task, "_stem_workspace")
        else:
            module = import_module(task.__module__)
        return IWorkspace.module_workspace(module)

    @staticmethod
    def module_workspace(module: ModuleType) -> Type["IWorkspace"]:
        if hasattr(module, "_stem_workspace"):
            return getattr(module, "_stem_workspace")
        else:
            tasks = {}
            workspaces = []
            for s in dir(module):
                t = getattr(module, s)
                try:
                    mro = [s.__name__ for s in t.__mro__]
                except:
                    mro = [s.__name__ for s in t.__class__.__mro__]
                if 'Task' in mro:
                    tasks[s] = t
                elif mro[1] == 'ILocalWorkspace':
                    workspaces.append(t)
            setattr(module, "_stem_workspace", LocalWorkspace(module.__name__, tasks, workspaces))
            return getattr(module, "_stem_workspace")


class ILocalWorkspace(IWorkspace):
    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):
    def __init__(self, name, tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta, ILocalWorkspace):
    def __new__(mcs, name, bases, attrs):
        if ILocalWorkspace not in bases:
            bases += (ILocalWorkspace,)

        cls = super().__new__(mcs, name, bases, attrs)
        try:
            workspaces = list(cls.workspaces)
        except AttributeError:
            workspaces = []

        cls_attr_replaced = {
            a: ProxyTask(a, t)
            for a, t in attrs.items()
            if isinstance(t, Task) and getattr(cls, a).__module__ != cls.__module__
        }

        for a, t in cls_attr_replaced.items():
            setattr(cls, a, t)

        for a, t in cls.__dict__.items():
            if isinstance(t, Task):
                t._stem_workspace = cls()

        tasks = {a: t for a, t in cls.__dict__.items() if isinstance(t, Task)}
        cls._workspaces = workspaces
        cls._tasks = tasks
        cls._name = name

        def __call(userclass):
            return userclass

        cls.__call__ = __call
        return cls()