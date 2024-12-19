from typing import TypeVar, Optional, Generic

from stem.task import Task
from stem.workspace import IWorkspace

T = TypeVar("T")


class TaskNode(Generic[T]):
    def __init__(self, task: Task[T], workspace: Optional[IWorkspace] = None):
        self.task = task
        if workspace is None:
            wrs = IWorkspace.find_default_workspace(task)
        else:
            wrs = workspace
        self.workspace = wrs
        self._dependencies = self.set_dependencies()
        self._unresolved_dependencies = self.set_unresolved_dependencies()
        self._has_dependence_errors = self.set_has_dependence_errors()

    @property
    def dependencies(self) -> list["TaskNode"]:
        return self._dependencies

    @property
    def unresolved_dependencies(self) -> list["str"]:
        return self._unresolved_dependencies

    @property
    def is_leaf(self) -> bool:
        if self.dependencies:
            return False
        return True

    @property
    def has_dependence_errors(self) -> bool:
        return self._has_dependence_errors

    def set_dependencies(self) -> list["TaskNode"]:
        resolved_dependencies = []
        for d in self.task.dependencies:
            if self.workspace.has_task(d):
                resolved_dependencies.append(
                    TaskNode(self.workspace.find_task(d), self.workspace)
                )
        return resolved_dependencies

    def set_unresolved_dependencies(self) -> list["str"]:
        unresolved_dependencies = []

        for d in self.task.dependencies:
            if not self.workspace.has_task(d):
                unresolved_dependencies.append(d)
        self._unresolved_dependencies = unresolved_dependencies
        return unresolved_dependencies

    def set_has_dependence_errors(self) -> bool:
        flag = False
        if self.unresolved_dependencies:
            return True
        if not flag:
            for d in self.dependencies:
                flag = d.has_dependence_errors
                if flag:
                    return True
        return flag


class TaskTree:
    def __init__(self, root: Task, workspace=None):
        self.root = TaskNode(root, workspace)

    def find_task(self, task, workspace=None) -> TaskNode[T]:
        if workspace is None:
            wrs = IWorkspace.find_default_workspace(task)
        else:
            wrs = workspace
        if task == self.root.task and wrs == self.root.workspace:
            return self.root
        else:
            for dep in self.root.dependencies:
                tree = TaskTree(dep.task, dep.workspace)
                node = tree.find_task(task, workspace)
                if node is not None:
                    return node
        return None

    def resolve_node(self, task: Task[T], workspace: Optional[IWorkspace] = None) -> TaskNode[T]:
        if workspace is None:
            wrs = IWorkspace.find_default_workspace(task)
        else:
            wrs = workspace
        node = self.find_task(task, wrs)
        if node is None:
            return TaskNode(task, workspace)
        else:
            return node
