import asyncio
from enum import Enum, auto
from typing import Optional, Callable, TypeVar, Generic
from functools import cached_property
from dataclasses import dataclass, field

from stem.meta import Meta, MetaVerification, Specification
from stem.task import Task
from stem.workspace import Workspace
from stem.task_runner import TaskRunner, SimpleRunner
from stem.task_tree import TaskNode, TaskTree

T = TypeVar("T")


@dataclass
class TaskMetaError(Generic[T]):
    task_node: TaskNode[T]
    meta_error: Optional[MetaVerification] = None
    user_handler_error: Optional[Exception] = None
    dependencies_error: list["TaskMetaError"] = field(default_factory=list)

    @property
    def task(self) -> Task[T]:
        return self.task_node.task

    @property
    def specification(self) -> Specification:
        return self.task.specification

    @property
    def has_error(self) -> bool:
        return self.meta_error is not None or \
            any(map(lambda x: x.has_error, self.dependencies_error))


class TaskStatus(Enum):
    DEPENDENCIES_ERROR = auto()
    META_ERROR = auto()
    INVOCATION_ERROR = auto()
    CONTAINS_DATA = auto()


@dataclass
class TaskResult(Generic[T]):
    status: TaskStatus
    task_node: TaskNode[T]
    meta_errors: Optional[TaskMetaError] = None
    lazy_data: Callable[[], T] = lambda: None

    @cached_property
    def data(self) -> Optional[T]:
        try:
            return self.lazy_data()
        except Exception as e:
            self.status = TaskStatus.INVOCATION_ERROR
            raise e


class TaskMaster:

    def __init__(self, task_runner: TaskRunner[T] = SimpleRunner(), task_tree: Optional[TaskTree] = None):
        self.task_runner = task_runner
        self.task_tree = task_tree

    def execute(self, meta: Meta, task: Task[T], workspace: Optional[Workspace] = None) -> TaskResult[T]:
        if self.task_tree is None:
            node = TaskNode(task, workspace)
        else:
            node = self.task_tree.resolve_node(task, workspace)
        if node.has_dependence_errors:
            return TaskResult(task_node=node, status=TaskStatus.DEPENDENCIES_ERROR)

        if task.specification is not None:
            meta_verify = MetaVerification.verify(
                meta=meta, specification=task.specification)
            if not meta_verify.checked_success:
                return TaskResult(task_node=node, status=TaskStatus.META_ERROR,
                                  meta_errors=TaskMetaError(task_node=node, meta_error=meta_verify))
        return TaskResult(task_node=node, status=TaskStatus.CONTAINS_DATA,
                          lazy_data=lambda: asyncio.run(
                              self.task_runner.run(meta, node))
                          if asyncio.iscoroutinefunction(self.task_runner.run)
                          else self.task_runner.run(meta, node))
