//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <quince/detail/binomen.h>
#include <quince/mappers/detail/persistent_column_mapper.h>
#include <quince/exprn_mappers/detail/exprn_mapper.h>
#include <quince/exprn_mappers/collective.h>
#include <quince/detail/compiler_specific.h>
#include <quince/query.h>
#include <quince_postgresql/database.h>
#include <quince_postgresql/detail/dialect_sql.h>

using namespace quince;
using boost::optional;
using std::make_unique;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;


namespace quince_postgresql {

dialect_sql::dialect_sql(const database &db) :
    sql(db),
    _next_placeholder_serial(0)
{}

unique_ptr<cloneable>
dialect_sql::clone_impl() const {
    return make_unique<dialect_sql>(*this);
}

void
dialect_sql::write_collective_comparison(relation r, const abstract_column_sequence &lhs, const collective_base &rhs) {
    const size_t n_cols = lhs.size();
    assert(n_cols != 0);

    if (n_cols != 1)  write("ROW (");

    comma_separated_list_scope list_scope(*this);
    lhs.for_each_column([&](const column_mapper &c) {
        list_scope.start_item();
        write_evaluation(c);
    });

    if (n_cols > 1)  write(")");

    write(" " + relop(r) + " ");

    switch(rhs.get_type()) {
        case collective_type::all:  write("ALL "); break;
        case collective_type::some: write("SOME "); break;
        default:                    abort();
    }
    write_subquery_exprn(rhs.get_query());
}

void
dialect_sql::write_nulls_low(bool invert) {
    write(" NULLS ");
    write(invert ? "LAST" : "FIRST");
}

void
dialect_sql::write_create_schema(const std::string &schema_name) {
    write("CREATE SCHEMA ");
    write_quoted(schema_name);
}

void
dialect_sql::write_returning(const abstract_mapper_base &mapper) {
    write(" RETURNING ");
    write_select_list(mapper);
}

void
dialect_sql::write_create_index(
    const binomen &table,
    size_t per_table_index_count,
    const vector<const abstract_mapper_base *> &mappers,
    bool unique
) {
    write("CREATE ");
    if (unique)  write("UNIQUE ");
    write("INDEX ON ");
    write_quoted(table);
    write(" (");
    expression_restriction_scope restriction_scope(*this, table._local);
    comma_separated_list_scope list_scope(*this);
    for (const abstract_mapper_base *m: mappers) {
        const auto pair = m->dissect_as_order_specification();
        const abstract_mapper_base * const stripped = pair.first;
        bool invert = pair.second;

        stripped->for_each_column([&](const column_mapper &c) {
            list_scope.start_item();
            if (dynamic_cast<const exprn_mapper_base *>(&c) == nullptr)
                write_evaluation(c);
            else {
                write("(");
                write_evaluation(c);
                write(")");
            }
            if (invert)  write(" DESC");
        });
    }
    write(")");
}

void
dialect_sql::write_distinct(const vector<const abstract_mapper_base *> &distincts) {
    write_distinct();
    if (! distincts.empty()) {
        write("ON (");
        comma_separated_list_scope list_scope(*this);
        for (const auto d: distincts)
            d->for_each_column([&](const column_mapper &c) {
                list_scope.start_item();
                write_evaluation(c);
            });
        write(")");
    }
    write(" ");
}

void
dialect_sql::write_add_columns(
    const binomen &table,
    const abstract_mapper_base &mapper,
    optional<column_id> generated_key
) {
    write_alter_table(table);
    sql::comma_separated_list_scope list_scope(*this);
    mapper.for_each_persistent_column([&](const persistent_column_mapper &p) {
        list_scope.start_item();
        write("ADD COLUMN ");
        write_title(p, generated_key);
    });
}

void
dialect_sql::write_drop_columns(const binomen &table, const abstract_mapper_base &mapper) {
    write_alter_table(table);
    sql::comma_separated_list_scope list_scope(*this);
    mapper.for_each_persistent_column([&](const persistent_column_mapper &p) {
        list_scope.start_item();
        write(" DROP COLUMN ");
        write_quoted(p.name());
    });
}

void
dialect_sql::write_rename_column(const binomen &table, const string &before, const string &after) {
    write_alter_table(table);
    write(" RENAME COLUMN ");
    write_quoted(before);
    write(" TO ");
    write_quoted(after);
}

void
dialect_sql::write_set_columns_types(
    const binomen &table,
    const abstract_mapper_base &mapper,
    optional<column_id> generated_key
) {
    write_alter_table(table);
    sql::comma_separated_list_scope list_scope(*this);
    mapper.for_each_persistent_column([&](const persistent_column_mapper &p) {
        const bool is_generated = generated_key == p.id();

        list_scope.start_item();
        write(" ALTER COLUMN ");
        write_quoted(p.name());
        write(" TYPE ");
        write(column_type_name(p.get_column_type(is_generated)));
    });
}

void
dialect_sql::write_fetch(const string &cursor_name, uint32_t n_rows) {
    write("FETCH FORWARD " + to_string(n_rows) + " IN " + cursor_name);
}

void
dialect_sql::prepend_declare_cursor(const string &cursor_name) {
    sql::text_insertion_scope before(*this, 0);
    write("DECLARE " + cursor_name + " CURSOR WITH HOLD FOR ");
}

void
dialect_sql::write_close_cursor(const string &cursor_name) {
    write("CLOSE " + cursor_name);
}

void
dialect_sql::write_set_session_characteristics(isolation_level isolation) {
    write("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ");
    switch (isolation) {
        case isolation_level::serializable:     write("SERIALIZABLE");      break;
        case isolation_level::repeatable_read:  write("REPEATABLE READ");   break;
        case isolation_level::read_committed:   write("READ COMMITTED");    break;
        case isolation_level::read_uncommitted: write("READ UNCOMMITTED");  break;
        default:                                abort();
    }
}

string
dialect_sql::next_placeholder() {
    return "$" + to_string(++_next_placeholder_serial);
}

}