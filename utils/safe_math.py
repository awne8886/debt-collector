"""A tiny, safe arithmetic evaluator for the counting game.

The counting channel used to hand raw message text to ``sympy.sympify``, which calls
``eval`` and therefore ran arbitrary Python. This module replaces it with a strict
whitelist over Python's own AST: only number literals, ``+ - * / // % **``, unary
``+``/``-`` and the calls ``sqrt(x)`` / ``abs(x)`` are accepted. Nothing is ever
executed; the tree is walked and computed by hand, with limits that keep the work tiny.

``safe_eval`` never raises: anything rejected or out of range returns ``None``.
"""

from __future__ import annotations

import ast
import math
from typing import Optional, Union

Number = Union[int, float]

MAX_EXPR_CHARS = 100
MAX_NODES = 40
MAX_ABS_VALUE = 10**15
MAX_POW_BASE = 10**6
MAX_POW_EXPONENT = 64

_FUNCTIONS = frozenset({"sqrt", "abs"})


class _Rejected(Exception):
    """Internal signal: the expression is not allowed or out of range."""


def _check(value: Number) -> Number:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _Rejected
    if isinstance(value, float) and not math.isfinite(value):
        raise _Rejected
    if abs(value) > MAX_ABS_VALUE:
        raise _Rejected
    return value


def _sqrt(value: Number) -> Number:
    if value < 0:
        raise _Rejected
    if isinstance(value, int):
        root = math.isqrt(value)
        if root * root == value:
            return root
    return math.sqrt(value)


def _pow(base: Number, exponent: Number) -> Number:
    if isinstance(exponent, bool) or not isinstance(exponent, int):
        raise _Rejected
    if not 0 <= exponent <= MAX_POW_EXPONENT:
        raise _Rejected
    if abs(base) > MAX_POW_BASE:
        raise _Rejected
    # Both operands are bounded, so this is at most 10**384: cheap to compute, then
    # rejected by _check. A float base that overflows raises OverflowError (caught).
    return base**exponent


def _eval(node: ast.AST) -> Number:
    if isinstance(node, ast.Expression):
        return _eval(node.body)

    if isinstance(node, ast.Constant):
        value = node.value
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _Rejected
        return _check(value)

    if isinstance(node, ast.UnaryOp):
        operand = _eval(node.operand)
        if isinstance(node.op, ast.UAdd):
            return _check(+operand)
        if isinstance(node.op, ast.USub):
            return _check(-operand)
        raise _Rejected

    if isinstance(node, ast.BinOp):
        op = node.op
        if not isinstance(
            op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow)
        ):
            raise _Rejected
        left = _eval(node.left)
        right = _eval(node.right)
        if isinstance(op, ast.Add):
            return _check(left + right)
        if isinstance(op, ast.Sub):
            return _check(left - right)
        if isinstance(op, ast.Mult):
            return _check(left * right)
        if isinstance(op, ast.Pow):
            return _check(_pow(left, right))
        if right == 0:
            raise _Rejected
        if isinstance(op, ast.Div):
            return _check(left / right)
        if isinstance(op, ast.FloorDiv):
            return _check(left // right)
        return _check(left % right)

    if isinstance(node, ast.Call):
        func = node.func
        if (
            not isinstance(func, ast.Name)
            or func.id not in _FUNCTIONS
            or len(node.args) != 1
            or node.keywords
        ):
            raise _Rejected
        argument = _eval(node.args[0])
        if func.id == "abs":
            return _check(abs(argument))
        return _check(_sqrt(argument))

    # Name, Attribute, Subscript, Lambda, comprehensions, Compare, BoolOp,
    # JoinedStr, Starred, ... are all refused.
    raise _Rejected


def safe_eval(text: str) -> Optional[Number]:
    """Evaluate a restricted arithmetic expression, or return ``None``.

    ``^`` is treated as power (as sympy did). The input is lowercased and stripped.
    """
    try:
        if not isinstance(text, str):
            return None
        expr = text.strip().lower()
        if not expr or len(expr) > MAX_EXPR_CHARS:
            return None
        expr = expr.replace("^", "**")
        tree = ast.parse(expr, mode="eval")
        if sum(1 for _ in ast.walk(tree)) > MAX_NODES:
            return None
        return _eval(tree)
    except Exception:
        # _Rejected, SyntaxError, OverflowError, RecursionError, MemoryError, ...
        return None


def safe_eval_int(text: str) -> Optional[int]:
    """Like :func:`safe_eval`, but only whole numbers count; floats must be integral."""
    value = safe_eval(text)
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        if value.is_integer():
            return int(value)
    except (OverflowError, ValueError):
        return None
    return None
