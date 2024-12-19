from functools import reduce
from typing import TypeVar, Union, Tuple, Callable, Optional, Generic, Any, Iterator

from abc import ABC, abstractmethod
from stem.core import Named
from stem.meta import Specification, Meta

T = TypeVar("T")


class Task(ABC, Generic[T], Named):
    dependencies: Tuple[Union[str, "Task"], ...]
    specification: Optional[Specification] = None
    settings: Optional[Meta] = None

    def check_by_meta(self, meta: Meta):
        pass

    @abstractmethod
    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        pass


class FunctionTask(Task[T]):
    def __init__(self, name: str, func: Callable, dependencies: Tuple[Union[str, "Task"], ...],
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.dependencies = dependencies
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        try:
            return self._func(meta, **kwargs)
        except:
            return self._func(self, meta, **kwargs)


class DataTask(Task[T]):
    dependencies = ()

    @abstractmethod
    def data(self, meta: Meta) -> T:
        pass

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self.data(meta)


class FunctionDataTask(DataTask[T]):
    def __init__(self, name: str = None, func: Callable = None,
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def data(self, meta: Meta) -> T:
        try:
            return self._func(meta)
        except:
            return self._func(self, meta)


def data(func: Callable[[Meta], T], specification: Optional[Specification] = None, **settings) -> FunctionDataTask[T]:
    def do():
        return FunctionDataTask(func.__name__, func, specification, **settings)

    fdt = do()
    fdt.__module__ = func.__module__
    return fdt


def task(func: Callable[[Meta, ...], T], specification: Optional[Specification] = None, **settings) -> FunctionTask[T]:
    def do():
        return FunctionTask(func.__name__,
                            func,
                            tuple(i for i in func.__annotations__.keys() if i != 'meta' and i != "return"),
                            specification,
                            **settings)

    FunctionTask.__module__ = func.__module__
    return do()


class MapTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "map_" + dependence.name
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        dp = self.dependencies if isinstance(self.dependencies, str) else self.dependencies.name
        return map(self._func, kwargs[dp])


class FilterTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "filter_" + dependence.name
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        dp = self.dependencies if isinstance(self.dependencies, str) else self.dependencies.name
        return filter(self._func, kwargs[dp])


class ReduceTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "reduce_" + dependence.name
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        dp = self.dependencies if isinstance(self.dependencies, str) else self.dependencies.name
        return reduce(self._func, kwargs[dp])