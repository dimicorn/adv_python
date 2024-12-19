import os
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool as Pool
from typing import Generic, TypeVar
from abc import ABC, abstractmethod

from stem.meta import Meta, get_meta_attr
from stem.task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        dc = {}
        for dep in task_node.dependencies:
            meta_new = get_meta_attr(meta, dep.task.name, {})
            dc[dep.task.name] = self.run(meta_new, dep)
        return task_node.task.transform(meta, **dc)


class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        dc = {}
        meta_new = [get_meta_attr(meta, dep.task.name, {})
                    for dep in task_node.dependencies]
        with ThreadPoolExecutor(max_workers=ThreadingRunner.MAX_WORKERS) as executor:
            lst = list(executor.map(
                self.run, meta_new, task_node.dependencies))
        dc = {task_node.dependencies[i].task.name: lst[i]
              for i in range(len(lst))}
        return task_node.task.transform(meta, **dc)


class AsyncRunner(TaskRunner[T]):
    async def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        dc = {}
        for dep in task_node.dependencies:
            meta_new = get_meta_attr(meta, dep.task.name, {})
            dc[dep.task.name] = await self.run(meta_new, dep)
        return task_node.task.transform(meta, **dc)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        dc = {}
        meta_new = [get_meta_attr(meta, dep.task.name, {})
                    for dep in task_node.dependencies]
        with Pool(ProcessingRunner.MAX_WORKERS) as p:
            lst = p.starmap(self.run, list(
                zip(meta_new, task_node.dependencies)))
        dc = {task_node.dependencies[i].task.name: lst[i]
              for i in range(len(lst))}
        return task_node.task.transform(meta, **dc)
