#ifndef QUINCE_POSTGRESQL__detail__dialect_sql_h
#define QUINCE_POSTGRESQL__detail__dialect_sql_h

//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../../../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

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

    virtual void write_select_list_item(const quince::column_mapper &) override;

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

    void write_create_schema(const std::string &);

    void write_fetch(const std::string &cursor_name, uint32_t n_rows);
    void prepend_declare_cursor(const std::string &cursor_name);
    void write_close_cursor(const std::string &cursor_name);
    void write_set_session_characteristics(isolation_level);

private:
    virtual void attach_value(const quince::cell &) override;
    virtual std::string next_placeholder() override;
    virtual std::string next_value_reference(const quince::cell &) override;

    uint32_t _next_placeholder_serial;
};

}

#endif
