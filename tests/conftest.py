from district42.types import Schema
from valera import Formatter, validate


def pytest_assertrepr_compare(op, left, right):
    if isinstance(right, Schema):
        result = validate(right, left)
        formatter = Formatter()
        errors = ["- " + e.format(formatter) for e in result.get_errors()]
        return ["valera.ValidationException"] + errors
