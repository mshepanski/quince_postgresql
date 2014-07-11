#ifndef QUINCE_POSTGRESQL__database_h
#define QUINCE_POSTGRESQL__database_h

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

#include <boost/optional.hpp>
#include <quince/database.h>
#include <quince/mapping_customization.h>
#include <quince_postgresql/detail/session.h>


namespace quince_postgresql {

class table_base;
class dialect_sql;

// See http://quince-lib.com/quince_postgresql.html#quince_postgresql.constructor
//
class database : public quince::database {
public:
    database(
        const std::string &host,
        const std::string &user,
        const std::string &password,
        const std::string &db_name = "",
        const std::string &default_schema = "",
        const std::string &port = "",
        const boost::optional<isolation_level> = boost::none,
        boost::optional<const quince::mapping_customization &> customization_for_db = boost::none
    );

    virtual ~database();


    // --- Everything from here to end of class is for quince internal use only. ---

    virtual std::unique_ptr<quince::sql>    make_sql() const override;
    virtual boost::optional<std::string>    get_default_enclosure() const override;
    void                                    make_enclosure_available(const boost::optional<std::string> &enclosure_name) const override;
    virtual quince::new_session             make_session() const override;
    virtual std::vector<std::string>        retrieve_column_titles(const quince::binomen &table) const override;
    virtual quince::column_type             retrievable_column_type(quince::column_type declared) const override;
    virtual quince::serial                  insert_with_readback(std::unique_ptr<quince::sql> insert, const quince::serial_mapper &readback_mapper) const override;
    virtual std::string                     column_type_name(quince::column_type) const override;
    virtual boost::optional<size_t>         max_column_name_length() const override;

    virtual bool supports_join(quince::conditional_junction_type) const override        { return true; }
    virtual bool supports_combination(quince::combination_type, bool) const override    { return true; }
    virtual bool supports_nested_combinations() const override                          { return true; }
    virtual bool supports_index(const quince::index_spec &) const override              { return true; }
    virtual bool imposes_combination_precedence() const override                        { return true; }

    std::unique_ptr<dialect_sql> make_dialect_sql() const;

    void create_schema(const std::string &schema_name) const;
    bool create_schema_if_not_exists(const boost::optional<std::string> &schema_name) const;

private:
    std::unique_ptr<session_impl> make_schemaless_session() const;
    std::shared_ptr<session_impl> get_session_impl() const;

    const session_impl::spec _spec;
    mutable std::set<std::string> _named_schemas_known_to_exist;
};

}

#endif
