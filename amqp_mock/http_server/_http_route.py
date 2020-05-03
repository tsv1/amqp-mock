__all__ = ("HttpRoute", "route",)

from typing import Any


class HttpRoute:
    def __init__(self, method: str, path: str) -> None:
        self._method = method
        self._path = path

    @property
    def method(self) -> str:
        return self._method

    @property
    def path(self) -> str:
        return self._path

    @classmethod
    def get_route(cls, handler: Any) -> Any:
        return getattr(handler, "__http_route__", None)

    def __call__(self, fn: Any) -> Any:
        setattr(fn, "__http_route__", self)
        return fn


def route(method: str, path: str) -> HttpRoute:
    return HttpRoute(method, path)
