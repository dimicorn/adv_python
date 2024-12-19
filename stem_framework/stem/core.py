from typing import Optional, Any, Protocol
import re


def pascal_case_to_snake_case(name: str) -> str:
    """
    Return current class in snake_case
    """
    name = re.sub("(.)([A-Z][a-z]+)", r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        """
        Returns value of private variable _name if it is not None and returns name of current class in snake_case
        """
        if self._name is not None:
            return self._name
        else:
            return pascal_case_to_snake_case(self.__class__.__name__)


class Dataclass(Protocol):
    """
    Dataclass protocol
    """
    __dataclass_fields__: Any
