#ifndef QUINCE_POSTGRESQL__detail__dialect_sql_h
#define QUINCE_POSTGRESQL__detail__dialect_sql_h

/*
    Copyright 2014 Michael Shepanski

    This file is part of the quince_postgresql library.

    Quince_postgresql is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Quince_postgresql is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with quince_postgresql.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <quince/detail/sql.h>


namespace quince_postgresql {

class database;
enum class isolation_level;

class dialect_sql : public quince::sql {
public:
    explicit dialect_sql(const database &);

    virtual std::unique_ptr<quince::cloneable> clone_impl() const override;

    virtual void write_no_limit() override  {}

    virtual void write_collective_comparison(
        quince::relation r,
        const quince::abstract_column_sequence &lhs,
        const quince::collective_base &rhs
    ) override;

    virtual void write_nulls_low(bool invert) override;

    virtual void write_returning(const quince::abstract_mapper_base &) override;

    virtual void
    write_create_index(
        const quince::binomen &table,
        size_t per_table_index_count,
        const std::vector<const quince::abstract_mapper_base *> &,
        bool unique
    ) override;

    virtual void write_distinct(const std::vector<const quince::abstract_mapper_base*> &) override;
    using quince::sql::write_distinct;

    void
    write_add_columns(
        const quince::binomen &table,
        const quince::abstract_mapper_base &mapper,
        boost::optional<quince::column_id> generated_key
    ) override;

    virtual void
    write_drop_columns(
        const quince::binomen &table,
        const quince::abstract_mapper_base &
    ) override;
    
    virtual void
    write_rename_column(
        const quince::binomen &table,
        const std::string &before,
        const std::string &after
    ) override;
    
    virtual void
    write_set_columns_types(
        const quince::binomen &table,
        const quince::abstract_mapper_base &,
        boost::optional<quince::column_id> generated_key
    ) override;

    virtual std::string next_placeholder() override;

    void write_create_schema(const std::string &);

    void write_fetch(const std::string &cursor_name, uint32_t n_rows);
    void prepend_declare_cursor(const std::string &cursor_name);
    void write_close_cursor(const std::string &cursor_name);
    void write_set_session_characteristics(isolation_level);

private:
    uint32_t _next_placeholder_serial;
};

}

#endif
