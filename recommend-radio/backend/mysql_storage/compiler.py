"""Strict AST dialect boundary for legacy query call sites, never regex rewriting."""

from __future__ import annotations

from functools import lru_cache

import sqlalchemy as sa
import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel
from sqlglot.tokens import TokenType


class UnsupportedBusinessSQL(ValueError):
    pass


def _dialect_node(node):
    if isinstance(node, exp.TimeToStr) and node.args.get("format") == exp.Literal.string("%s"):
        value = node.this
        if isinstance(value, exp.TsOrDsToTimestamp):
            value = value.this
        return exp.Anonymous(
            this="UNIX_TIMESTAMP",
            expressions=[
                exp.Anonymous(
                    this="STR_TO_DATE",
                    expressions=[
                        exp.Anonymous(
                            this="REPLACE",
                            expressions=[
                                exp.Substring(
                                    this=value,
                                    start=exp.Literal.number(1),
                                    length=exp.Literal.number(19),
                                ),
                                exp.Literal.string("T"),
                                exp.Literal.string(" "),
                            ],
                        ),
                        exp.Literal.string("%Y-%m-%d %H:%i:%s"),
                    ],
                )
            ],
        )
    if isinstance(node, exp.JSONExtract):
        # SQLite extracts scalar text without JSON quotation. Keep object/array
        # representations as text, and distinguish JSON null from the string
        # "null" so SQL IS NULL predicates keep their SQLite meaning.
        return exp.Case(
            ifs=[
                exp.If(
                    this=exp.EQ(
                        this=exp.Anonymous(this="JSON_TYPE", expressions=[node.copy()]),
                        expression=exp.Literal.string("NULL"),
                    ),
                    true=exp.Null(),
                )
            ],
            default=exp.Anonymous(this="JSON_UNQUOTE", expressions=[node.copy()]),
        )
    if isinstance(node, exp.Column) and node.table.lower() == "excluded":
        return exp.Anonymous(this="VALUES", expressions=[exp.column(node.name)])
    return node


def mysql_expression(node):
    mapped = node.transform(_dialect_node, copy=True)
    return mapped.sql(dialect="mysql", identify=True, unsupported_level=ErrorLevel.RAISE)


@lru_cache(maxsize=2048)
def parse_query(sql):
    tokens = sqlglot.tokenize(sql, read="sqlite")
    positions = [
        token for token in tokens if token.token_type == TokenType.PLACEHOLDER and token.text == "?"
    ]
    text = sql
    for index in range(len(positions) - 1, -1, -1):
        token = positions[index]
        text = text[: token.start] + f":p{index}" + text[token.end + 1 :]
    statements = sqlglot.parse(text, read="sqlite", error_level=ErrorLevel.RAISE)
    if len(statements) != 1 or statements[0] is None:
        raise UnsupportedBusinessSQL("Exactly one recognized SQL statement is required")
    tree = statements[0]
    if not isinstance(
        tree,
        (
            exp.Select,
            exp.Union,
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Transaction,
            exp.Commit,
            exp.Rollback,
            exp.Pragma,
            exp.Create,
        ),
    ):
        raise UnsupportedBusinessSQL(f"Unsupported business SQL node: {type(tree).__name__}")
    if tree.find(exp.Returning):
        raise UnsupportedBusinessSQL("RETURNING requires an explicit repository operation on MySQL")
    return tree, len(positions)


def bind_parameters(count, parameters):
    if isinstance(parameters, dict):
        if count:
            raise ValueError("Positional placeholders require a positional parameter sequence")
        return parameters
    values = tuple(parameters or ())
    if len(values) != count:
        raise ValueError(f"Expected {count} bound values, got {len(values)}")
    return {f"p{index}": value for index, value in enumerate(values)}


def expression_value(node):
    if isinstance(node, exp.Placeholder):
        return sa.bindparam(node.name)
    if isinstance(node, exp.Null):
        return sa.null()
    if isinstance(node, exp.Literal):
        return sa.literal(
            node.this
            if node.is_string
            else float(node.this)
            if "." in node.this
            else int(node.this)
        )
    return sa.text(mysql_expression(node))
