# sql/crud.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Functions used by compiler.py to determine the parameters rendered
within INSERT and UPDATE statements.

"""
import functools
import operator

from . import coercions
from . import dml
from . import elements
from . import roles
from .. import exc
from .. import util

REQUIRED = util.symbol(
    "REQUIRED",
    """
Placeholder for the value within a :class:`.BindParameter`
which is required to be present when the statement is passed
to :meth:`_engine.Connection.execute`.

This symbol is typically used when a :func:`_expression.insert`
or :func:`_expression.update` statement is compiled without parameter
values present.

""",
)


def _get_crud_params(compiler, stmt, compile_state, **kw):
    """create a set of tuples representing column/string pairs for use
    in an INSERT or UPDATE statement.

    Also generates the Compiled object's postfetch, prefetch, and
    returning column collections, used for default handling and ultimately
    populating the CursorResult's prefetch_cols() and postfetch_cols()
    collections.

    """

    compiler.postfetch = []
    compiler.insert_prefetch = []
    compiler.update_prefetch = []
    compiler.returning = []

    # getters - these are normally just column.key,
    # but in the case of mysql multi-table update, the rules for
    # .key must conditionally take tablename into account
    (
        _column_as_key,
        _getattr_col_key,
        _col_bind_name,
    ) = getters = _key_getters_for_crud_column(compiler, stmt, compile_state)

    compiler._key_getters_for_crud_column = getters

    # no parameters in the statement, no parameters in the
    # compiled params - return binds for all columns
    if compiler.column_keys is None and compile_state._no_parameters:
        return [
            (
                c,
                compiler.preparer.format_column(c),
                _create_bind_param(compiler, c, None, required=True),
            )
            for c in stmt.table.columns
        ]

    if compile_state._has_multi_parameters:
        spd = compile_state._multi_parameters[0]
        stmt_parameter_tuples = list(spd.items())
    elif compile_state._ordered_values:
        spd = compile_state._dict_parameters
        stmt_parameter_tuples = compile_state._ordered_values
    elif compile_state._dict_parameters:
        spd = compile_state._dict_parameters
        stmt_parameter_tuples = list(spd.items())
    else:
        stmt_parameter_tuples = spd = None

    # if we have statement parameters - set defaults in the
    # compiled params
    if compiler.column_keys is None:
        parameters = {}
    elif stmt_parameter_tuples:
        parameters = dict(
            (_column_as_key(key), REQUIRED)
            for key in compiler.column_keys
            if key not in spd
        )
    else:
        parameters = dict(
            (_column_as_key(key), REQUIRED) for key in compiler.column_keys
        )

    # create a list of column assignment clauses as tuples
    values = []

    if stmt_parameter_tuples is not None:
        _get_stmt_parameter_tuples_params(
            compiler,
            compile_state,
            parameters,
            stmt_parameter_tuples,
            _column_as_key,
            values,
            kw,
        )

    check_columns = {}

    # special logic that only occurs for multi-table UPDATE
    # statements
    if compile_state.isupdate and compile_state.is_multitable:
        _get_multitable_params(
            compiler,
            stmt,
            compile_state,
            stmt_parameter_tuples,
            check_columns,
            _col_bind_name,
            _getattr_col_key,
            values,
            kw,
        )

    if compile_state.isinsert and stmt._select_names:
        _scan_insert_from_select_cols(
            compiler,
            stmt,
            compile_state,
            parameters,
            _getattr_col_key,
            _column_as_key,
            _col_bind_name,
            check_columns,
            values,
            kw,
        )
    else:
        _scan_cols(
            compiler,
            stmt,
            compile_state,
            parameters,
            _getattr_col_key,
            _column_as_key,
            _col_bind_name,
            check_columns,
            values,
            kw,
        )

    if parameters and stmt_parameter_tuples:
        check = (
            set(parameters)
            .intersection(_column_as_key(k) for k, v in stmt_parameter_tuples)
            .difference(check_columns)
        )
        if check:
            raise exc.CompileError(
                "Unconsumed column names: %s"
                % (", ".join("%s" % (c,) for c in check))
            )

    if compile_state._has_multi_parameters:
        values = _extend_values_for_multiparams(
            compiler, stmt, compile_state, values, kw
        )
    elif not values and compiler.for_executemany:
        # convert an "INSERT DEFAULT VALUES"
        # into INSERT (firstcol) VALUES (DEFAULT) which can be turned
        # into an in-place multi values.  This supports
        # insert_executemany_returning mode :)
        values = [
            (
                stmt.table.columns[0],
                compiler.preparer.format_column(stmt.table.columns[0]),
                "DEFAULT",
            )
        ]

    return values


def _create_bind_param(
    compiler, col, value, process=True, required=False, name=None, **kw
):
    if name is None:
        name = col.key
    bindparam = elements.BindParameter(
        name, value, type_=col.type, required=required
    )
    bindparam._is_crud = True
    if process:
        bindparam = bindparam._compiler_dispatch(compiler, **kw)
    return bindparam


def _handle_values_anonymous_param(compiler, col, value, name, **kw):
    # the insert() and update() constructs as of 1.4 will now produce anonymous
    # bindparam() objects in the values() collections up front when given plain
    # literal values.  This is so that cache key behaviors, which need to
    # produce bound parameters in deterministic order without invoking any
    # compilation here, can be applied to these constructs when they include
    # values() (but not yet multi-values, which are not included in caching
    # right now).
    #
    # in order to produce the desired "crud" style name for these parameters,
    # which will also be targetable in engine/default.py through the usual
    # conventions, apply our desired name to these unique parameters by
    # populating the compiler truncated names cache with the desired name,
    # rather than having
    # compiler.visit_bindparam()->compiler._truncated_identifier make up a
    # name.  Saves on call counts also.
    if value.unique and isinstance(value.key, elements._truncated_label):
        compiler.truncated_names[("bindparam", value.key)] = name

    if value.type._isnull:
        # either unique parameter, or other bound parameters that were
        # passed in directly
        # set type to that of the column unconditionally
        value = value._with_binary_element_type(col.type)

    return value._compiler_dispatch(compiler, **kw)


def _key_getters_for_crud_column(compiler, stmt, compile_state):
    if compile_state.isupdate and compile_state._extra_froms:
        # when extra tables are present, refer to the columns
        # in those extra tables as table-qualified, including in
        # dictionaries and when rendering bind param names.
        # the "main" table of the statement remains unqualified,
        # allowing the most compatibility with a non-multi-table
        # statement.
        _et = set(compile_state._extra_froms)

        c_key_role = functools.partial(
            coercions.expect_as_key, roles.DMLColumnRole
        )

        def _column_as_key(key):
            str_key = c_key_role(key)
            if hasattr(key, "table") and key.table in _et:
                return (key.table.name, str_key)
            else:
                return str_key

        def _getattr_col_key(col):
            if col.table in _et:
                return (col.table.name, col.key)
            else:
                return col.key

        def _col_bind_name(col):
            if col.table in _et:
                return "%s_%s" % (col.table.name, col.key)
            else:
                return col.key

    else:
        _column_as_key = functools.partial(
            coercions.expect_as_key, roles.DMLColumnRole
        )
        _getattr_col_key = _col_bind_name = operator.attrgetter("key")

    return _column_as_key, _getattr_col_key, _col_bind_name


def _scan_insert_from_select_cols(
    compiler,
    stmt,
    compile_state,
    parameters,
    _getattr_col_key,
    _column_as_key,
    _col_bind_name,
    check_columns,
    values,
    kw,
):

    (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    ) = _get_returning_modifiers(compiler, stmt, compile_state)

    cols = [stmt.table.c[_column_as_key(name)] for name in stmt._select_names]

    compiler._insert_from_select = stmt.select

    add_select_cols = []
    if stmt.include_insert_from_select_defaults:
        col_set = set(cols)
        for col in stmt.table.columns:
            if col not in col_set and col.default:
                cols.append(col)

    for c in cols:
        col_key = _getattr_col_key(c)
        if col_key in parameters and col_key not in check_columns:
            parameters.pop(col_key)
            values.append((c, compiler.preparer.format_column(c), None))
        else:
            _append_param_insert_select_hasdefault(
                compiler, stmt, c, add_select_cols, kw
            )

    if add_select_cols:
        values.extend(add_select_cols)
        compiler._insert_from_select = compiler._insert_from_select._generate()
        compiler._insert_from_select._raw_columns = tuple(
            compiler._insert_from_select._raw_columns
        ) + tuple(expr for col, col_expr, expr in add_select_cols)


def _scan_cols(
    compiler,
    stmt,
    compile_state,
    parameters,
    _getattr_col_key,
    _column_as_key,
    _col_bind_name,
    check_columns,
    values,
    kw,
):
    (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    ) = _get_returning_modifiers(compiler, stmt, compile_state)

    if compile_state._parameter_ordering:
        parameter_ordering = [
            _column_as_key(key) for key in compile_state._parameter_ordering
        ]
        ordered_keys = set(parameter_ordering)
        cols = [
            stmt.table.c[key]
            for key in parameter_ordering
            if isinstance(key, util.string_types) and key in stmt.table.c
        ] + [c for c in stmt.table.c if c.key not in ordered_keys]

    else:
        cols = stmt.table.columns

    for c in cols:
        # scan through every column in the target table

        col_key = _getattr_col_key(c)

        if col_key in parameters and col_key not in check_columns:
            # parameter is present for the column.  use that.

            _append_param_parameter(
                compiler,
                stmt,
                compile_state,
                c,
                col_key,
                parameters,
                _col_bind_name,
                implicit_returning,
                implicit_return_defaults,
                values,
                kw,
            )

        elif compile_state.isinsert:
            # no parameter is present and it's an insert.

            if c.primary_key and need_pks:
                # it's a primary key column, it will need to be generated by a
                # default generator of some kind, and the statement expects
                # inserted_primary_key to be available.

                if implicit_returning:
                    # we can use RETURNING, find out how to invoke this
                    # column and get the value where RETURNING is an option.
                    # we can inline server-side functions in this case.

                    _append_param_insert_pk_returning(
                        compiler, stmt, c, values, kw
                    )
                else:
                    # otherwise, find out how to invoke this column
                    # and get its value where RETURNING is not an option.
                    # if we have to invoke a server-side function, we need
                    # to pre-execute it.   or if this is a straight
                    # autoincrement column and the dialect supports it
                    # we can use cursor.lastrowid.

                    _append_param_insert_pk_no_returning(
                        compiler, stmt, c, values, kw
                    )

            elif c.default is not None:
                # column has a default, but it's not a pk column, or it is but
                # we don't need to get the pk back.
                _append_param_insert_hasdefault(
                    compiler, stmt, c, implicit_return_defaults, values, kw
                )

            elif c.server_default is not None:
                # column has a DDL-level default, and is either not a pk
                # column or we don't need the pk.
                if implicit_return_defaults and c in implicit_return_defaults:
                    compiler.returning.append(c)
                elif not c.primary_key:
                    compiler.postfetch.append(c)
            elif implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            elif (
                c.primary_key
                and c is not stmt.table._autoincrement_column
                and not c.nullable
            ):
                _warn_pk_with_no_anticipated_value(c)

        elif compile_state.isupdate:
            # no parameter is present and it's an insert.

            _append_param_update(
                compiler,
                compile_state,
                stmt,
                c,
                implicit_return_defaults,
                values,
                kw,
            )


def _append_param_parameter(
    compiler,
    stmt,
    compile_state,
    c,
    col_key,
    parameters,
    _col_bind_name,
    implicit_returning,
    implicit_return_defaults,
    values,
    kw,
):

    value = parameters.pop(col_key)

    col_value = compiler.preparer.format_column(
        c, use_table=compile_state.include_table_with_column_exprs
    )

    if coercions._is_literal(value):
        value = _create_bind_param(
            compiler,
            c,
            value,
            required=value is REQUIRED,
            name=_col_bind_name(c)
            if not compile_state._has_multi_parameters
            else "%s_m0" % _col_bind_name(c),
            **kw
        )
    elif value._is_bind_parameter:
        value = _handle_values_anonymous_param(
            compiler,
            c,
            value,
            name=_col_bind_name(c)
            if not compile_state._has_multi_parameters
            else "%s_m0" % _col_bind_name(c),
            **kw
        )
    else:
        # value is a SQL expression
        value = compiler.process(value.self_group(), **kw)

        if compile_state.isupdate:
            if implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)

            else:
                compiler.postfetch.append(c)
        else:
            if c.primary_key:

                if implicit_returning:
                    compiler.returning.append(c)
                elif compiler.dialect.postfetch_lastrowid:
                    compiler.postfetch_lastrowid = True

            elif implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)

            else:
                # postfetch specifically means, "we can SELECT the row we just
                # inserted by primary key to get back the server generated
                # defaults". so by definition this can't be used to get the
                # primary key value back, because we need to have it ahead of
                # time.

                compiler.postfetch.append(c)

    values.append((c, col_value, value))


def _append_param_insert_pk_returning(compiler, stmt, c, values, kw):
    """Create a primary key expression in the INSERT statement where
    we want to populate result.inserted_primary_key and RETURNING
    is available.

    """
    if c.default is not None:
        if c.default.is_sequence:
            if compiler.dialect.supports_sequences and (
                not c.default.optional
                or not compiler.dialect.sequences_optional
            ):
                values.append(
                    (
                        c,
                        compiler.preparer.format_column(c),
                        compiler.process(c.default, **kw),
                    )
                )
            compiler.returning.append(c)
        elif c.default.is_clause_element:
            values.append(
                (
                    c,
                    compiler.preparer.format_column(c),
                    compiler.process(c.default.arg.self_group(), **kw),
                )
            )
            compiler.returning.append(c)
        else:
            # client side default.  OK we can't use RETURNING, need to
            # do a "prefetch", which in fact fetches the default value
            # on the Python side
            values.append(
                (
                    c,
                    compiler.preparer.format_column(c),
                    _create_insert_prefetch_bind_param(compiler, c, **kw),
                )
            )
    elif c is stmt.table._autoincrement_column or c.server_default is not None:
        compiler.returning.append(c)
    elif not c.nullable:
        # no .default, no .server_default, not autoincrement, we have
        # no indication this primary key column will have any value
        _warn_pk_with_no_anticipated_value(c)


def _append_param_insert_pk_no_returning(compiler, stmt, c, values, kw):
    """Create a primary key expression in the INSERT statement where
    we want to populate result.inserted_primary_key and we cannot use
    RETURNING.

    Depending on the kind of default here we may create a bound parameter
    in the INSERT statement and pre-execute a default generation function,
    or we may use cursor.lastrowid if supported by the dialect.


    """

    if (
        # column has a Python-side default
        c.default is not None
        and (
            # and it either is not a sequence, or it is and we support
            # sequences and want to invoke it
            not c.default.is_sequence
            or (
                compiler.dialect.supports_sequences
                and (
                    not c.default.optional
                    or not compiler.dialect.sequences_optional
                )
            )
        )
    ) or (
        # column is the "autoincrement column"
        c is stmt.table._autoincrement_column
        and (
            # dialect can't use cursor.lastrowid
            not compiler.dialect.postfetch_lastrowid
            and (
                # column has a Sequence and we support those
                (
                    c.default is not None
                    and c.default.is_sequence
                    and compiler.dialect.supports_sequences
                )
                or
                # column has no default on it, but dialect can run the
                # "autoincrement" mechanism explicitly, e.g. PostgreSQL
                # SERIAL we know the sequence name
                (
                    c.default is None
                    and compiler.dialect.preexecute_autoincrement_sequences
                )
            )
        )
    ):
        # do a pre-execute of the default
        values.append(
            (
                c,
                compiler.preparer.format_column(c),
                _create_insert_prefetch_bind_param(compiler, c, **kw),
            )
        )
    elif (
        c.default is None
        and c.server_default is None
        and not c.nullable
        and c is not stmt.table._autoincrement_column
    ):
        # no .default, no .server_default, not autoincrement, we have
        # no indication this primary key column will have any value
        _warn_pk_with_no_anticipated_value(c)
    elif compiler.dialect.postfetch_lastrowid:
        # finally, where it seems like there will be a generated primary key
        # value and we haven't set up any other way to fetch it, and the
        # dialect supports cursor.lastrowid, switch on the lastrowid flag so
        # that the DefaultExecutionContext calls upon cursor.lastrowid
        compiler.postfetch_lastrowid = True


def _append_param_insert_hasdefault(
    compiler, stmt, c, implicit_return_defaults, values, kw
):
    if c.default.is_sequence:
        if compiler.dialect.supports_sequences and (
            not c.default.optional or not compiler.dialect.sequences_optional
        ):
            values.append(
                (
                    c,
                    compiler.preparer.format_column(c),
                    compiler.process(c.default, **kw),
                )
            )
            if implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            elif not c.primary_key:
                compiler.postfetch.append(c)
    elif c.default.is_clause_element:
        values.append(
            (
                c,
                compiler.preparer.format_column(c),
                compiler.process(c.default.arg.self_group(), **kw),
            )
        )

        if implicit_return_defaults and c in implicit_return_defaults:
            compiler.returning.append(c)
        elif not c.primary_key:
            # don't add primary key column to postfetch
            compiler.postfetch.append(c)
    else:
        values.append(
            (
                c,
                compiler.preparer.format_column(c),
                _create_insert_prefetch_bind_param(compiler, c, **kw),
            )
        )


def _append_param_insert_select_hasdefault(compiler, stmt, c, values, kw):

    if c.default.is_sequence:
        if compiler.dialect.supports_sequences and (
            not c.default.optional or not compiler.dialect.sequences_optional
        ):
            values.append(
                (c, compiler.preparer.format_column(c), c.default.next_value())
            )
    elif c.default.is_clause_element:
        values.append(
            (c, compiler.preparer.format_column(c), c.default.arg.self_group())
        )
    else:
        values.append(
            (
                c,
                compiler.preparer.format_column(c),
                _create_insert_prefetch_bind_param(
                    compiler, c, process=False, **kw
                ),
            )
        )


def _append_param_update(
    compiler, compile_state, stmt, c, implicit_return_defaults, values, kw
):

    include_table = compile_state.include_table_with_column_exprs
    if c.onupdate is not None and not c.onupdate.is_sequence:
        if c.onupdate.is_clause_element:
            values.append(
                (
                    c,
                    compiler.preparer.format_column(
                        c,
                        use_table=include_table,
                    ),
                    compiler.process(c.onupdate.arg.self_group(), **kw),
                )
            )
            if implicit_return_defaults and c in implicit_return_defaults:
                compiler.returning.append(c)
            else:
                compiler.postfetch.append(c)
        else:
            values.append(
                (
                    c,
                    compiler.preparer.format_column(
                        c,
                        use_table=include_table,
                    ),
                    _create_update_prefetch_bind_param(compiler, c, **kw),
                )
            )
    elif c.server_onupdate is not None:
        if implicit_return_defaults and c in implicit_return_defaults:
            compiler.returning.append(c)
        else:
            compiler.postfetch.append(c)
    elif (
        implicit_return_defaults
        and stmt._return_defaults is not True
        and c in implicit_return_defaults
    ):
        compiler.returning.append(c)


def _create_insert_prefetch_bind_param(
    compiler, c, process=True, name=None, **kw
):

    param = _create_bind_param(
        compiler, c, None, process=process, name=name, **kw
    )
    compiler.insert_prefetch.append(c)
    return param


def _create_update_prefetch_bind_param(
    compiler, c, process=True, name=None, **kw
):
    param = _create_bind_param(
        compiler, c, None, process=process, name=name, **kw
    )
    compiler.update_prefetch.append(c)
    return param


class _multiparam_column(elements.ColumnElement):
    _is_multiparam_column = True

    def __init__(self, original, index):
        self.index = index
        self.key = "%s_m%d" % (original.key, index + 1)
        self.original = original
        self.default = original.default
        self.type = original.type

    def compare(self, other, **kw):
        raise NotImplementedError()

    def _copy_internals(self, other, **kw):
        raise NotImplementedError()

    def __eq__(self, other):
        return (
            isinstance(other, _multiparam_column)
            and other.key == self.key
            and other.original == self.original
        )


def _process_multiparam_default_bind(compiler, stmt, c, index, kw):

    if not c.default:
        raise exc.CompileError(
            "INSERT value for column %s is explicitly rendered as a bound"
            "parameter in the VALUES clause; "
            "a Python-side value or SQL expression is required" % c
        )
    elif c.default.is_clause_element:
        return compiler.process(c.default.arg.self_group(), **kw)
    else:
        col = _multiparam_column(c, index)
        if isinstance(stmt, dml.Insert):
            return _create_insert_prefetch_bind_param(compiler, col, **kw)
        else:
            return _create_update_prefetch_bind_param(compiler, col, **kw)


def _get_multitable_params(
    compiler,
    stmt,
    compile_state,
    stmt_parameter_tuples,
    check_columns,
    _col_bind_name,
    _getattr_col_key,
    values,
    kw,
):
    normalized_params = dict(
        (coercions.expect(roles.DMLColumnRole, c), param)
        for c, param in stmt_parameter_tuples
    )

    include_table = compile_state.include_table_with_column_exprs

    affected_tables = set()
    for t in compile_state._extra_froms:
        for c in t.c:
            if c in normalized_params:
                affected_tables.add(t)
                check_columns[_getattr_col_key(c)] = c
                value = normalized_params[c]

                col_value = compiler.process(c, include_table=include_table)
                if coercions._is_literal(value):
                    value = _create_bind_param(
                        compiler,
                        c,
                        value,
                        required=value is REQUIRED,
                        name=_col_bind_name(c),
                        **kw  # TODO: no test coverage for literal binds here
                    )
                elif value._is_bind_parameter:
                    value = _handle_values_anonymous_param(
                        compiler, c, value, name=_col_bind_name(c), **kw
                    )
                else:
                    compiler.postfetch.append(c)
                    value = compiler.process(value.self_group(), **kw)
                values.append((c, col_value, value))
    # determine tables which are actually to be updated - process onupdate
    # and server_onupdate for these
    for t in affected_tables:
        for c in t.c:
            if c in normalized_params:
                continue
            elif c.onupdate is not None and not c.onupdate.is_sequence:
                if c.onupdate.is_clause_element:
                    values.append(
                        (
                            c,
                            compiler.process(c, include_table=include_table),
                            compiler.process(
                                c.onupdate.arg.self_group(), **kw
                            ),
                        )
                    )
                    compiler.postfetch.append(c)
                else:
                    values.append(
                        (
                            c,
                            compiler.process(c, include_table=include_table),
                            _create_update_prefetch_bind_param(
                                compiler, c, name=_col_bind_name(c), **kw
                            ),
                        )
                    )
            elif c.server_onupdate is not None:
                compiler.postfetch.append(c)


def _extend_values_for_multiparams(compiler, stmt, compile_state, values, kw):
    values_0 = values
    values = [values]

    for i, row in enumerate(compile_state._multi_parameters[1:]):
        extension = []
        for (col, col_expr, param) in values_0:
            if col in row or col.key in row:
                key = col if col in row else col.key

                if coercions._is_literal(row[key]):
                    new_param = _create_bind_param(
                        compiler,
                        col,
                        row[key],
                        name="%s_m%d" % (col.key, i + 1),
                        **kw
                    )
                else:
                    new_param = compiler.process(row[key].self_group(), **kw)
            else:
                new_param = _process_multiparam_default_bind(
                    compiler, stmt, col, i, kw
                )

            extension.append((col, col_expr, new_param))

        values.append(extension)

    return values


def _get_stmt_parameter_tuples_params(
    compiler,
    compile_state,
    parameters,
    stmt_parameter_tuples,
    _column_as_key,
    values,
    kw,
):

    for k, v in stmt_parameter_tuples:
        colkey = _column_as_key(k)
        if colkey is not None:
            parameters.setdefault(colkey, v)
        else:
            # a non-Column expression on the left side;
            # add it to values() in an "as-is" state,
            # coercing right side to bound param

            # note one of the main use cases for this is array slice
            # updates on PostgreSQL, as the left side is also an expression.

            col_expr = compiler.process(
                k, include_table=compile_state.include_table_with_column_exprs
            )

            if coercions._is_literal(v):
                v = compiler.process(
                    elements.BindParameter(None, v, type_=k.type), **kw
                )
            else:
                if v._is_bind_parameter and v.type._isnull:
                    # either unique parameter, or other bound parameters that
                    # were passed in directly
                    # set type to that of the column unconditionally
                    v = v._with_binary_element_type(k.type)

                v = compiler.process(v.self_group(), **kw)

            values.append((k, col_expr, v))


def _get_returning_modifiers(compiler, stmt, compile_state):

    need_pks = (
        compile_state.isinsert
        and not stmt._inline
        and (
            not compiler.for_executemany
            or (
                compiler.dialect.insert_executemany_returning
                and stmt._return_defaults
            )
        )
        and not stmt._returning
        and not compile_state._has_multi_parameters
    )

    implicit_returning = (
        need_pks
        and compiler.dialect.implicit_returning
        and stmt.table.implicit_returning
    )

    if compile_state.isinsert:
        implicit_return_defaults = implicit_returning and stmt._return_defaults
    elif compile_state.isupdate:
        implicit_return_defaults = (
            compiler.dialect.implicit_returning
            and stmt.table.implicit_returning
            and stmt._return_defaults
        )
    else:
        # this line is unused, currently we are always
        # isinsert or isupdate
        implicit_return_defaults = False  # pragma: no cover

    if implicit_return_defaults:
        if stmt._return_defaults is True:
            implicit_return_defaults = set(stmt.table.c)
        else:
            implicit_return_defaults = set(stmt._return_defaults)

    postfetch_lastrowid = need_pks and compiler.dialect.postfetch_lastrowid

    return (
        need_pks,
        implicit_returning,
        implicit_return_defaults,
        postfetch_lastrowid,
    )


def _warn_pk_with_no_anticipated_value(c):
    msg = (
        "Column '%s.%s' is marked as a member of the "
        "primary key for table '%s', "
        "but has no Python-side or server-side default generator indicated, "
        "nor does it indicate 'autoincrement=True' or 'nullable=True', "
        "and no explicit value is passed.  "
        "Primary key columns typically may not store NULL."
        % (c.table.fullname, c.name, c.table.fullname)
    )
    if len(c.table.primary_key) > 1:
        msg += (
            " Note that as of SQLAlchemy 1.1, 'autoincrement=True' must be "
            "indicated explicitly for composite (e.g. multicolumn) primary "
            "keys if AUTO_INCREMENT/SERIAL/IDENTITY "
            "behavior is expected for one of the columns in the primary key. "
            "CREATE TABLE statements are impacted by this change as well on "
            "most backends."
        )
    util.warn(msg)
