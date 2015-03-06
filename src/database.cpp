//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <pg_config_manual.h>  // for NAMEDATALEN
#include <quince/exceptions.h>
#include <quince/detail/compiler_specific.h>
#include <quince/detail/session.h>
#include <quince/transaction.h>
#include <quince/mappers/direct_mapper.h>
#include <quince/mappers/numeric_cast_mapper.h>
#include <quince/mappers/reinterpret_cast_mapper.h>
#include <quince/mappers/serial_mapper.h>
#include <quince_postgresql/database.h>
#include <quince_postgresql/detail/dialect_sql.h>

using boost::optional;
using namespace quince;
using std::dynamic_pointer_cast;
using std::make_unique;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;


namespace quince_postgresql {

namespace {
    struct customization_for_dbms : mapping_customization {
        customization_for_dbms() {
            customize<bool, direct_mapper<bool>>();
            customize<int16_t, direct_mapper<int16_t>>();
            customize<int32_t, direct_mapper<int32_t>>();
            customize<int64_t, direct_mapper<int64_t>>();
            customize<float, direct_mapper<float>>();
            customize<double, direct_mapper<double>>();
            customize<int8_t, numeric_cast_mapper<int8_t, direct_mapper<int16_t>>>();
            customize<uint8_t, numeric_cast_mapper<uint8_t, direct_mapper<int16_t>>>();
            customize<uint16_t, numeric_cast_mapper<uint16_t, direct_mapper<int32_t>>>();
            customize<uint32_t, numeric_cast_mapper<uint32_t, direct_mapper<int64_t>>>();
            customize<uint64_t, reinterpret_cast_mapper<uint64_t, direct_mapper<int64_t>, uint64_t(0x8000000000000000)>>();
            customize<std::string, direct_mapper<std::string>>();
            customize<byte_vector, direct_mapper<byte_vector>>();
            customize<serial, serial_mapper>();
        }
    };

    optional<std::string>
    to_optional(const std::string &s) {
        if (s.empty())  return boost::none;
        else            return s;
    }
}

database::database(
    const std::string &host,
    const std::string &user,
    const std::string &password,
    const std::string &db_name,
    const std::string &default_schema,
    const std::string &port,
    const optional<isolation_level> level,
    const boost::optional<mapping_customization> &customization_for_db
) :
    quince::database(
        clone_or_null(customization_for_db),
        std::make_unique<customization_for_dbms>()
    ),
    _spec({
        host,
        user,
        password,
        to_optional(db_name),
        to_optional(default_schema),
        to_optional(port),
        level
    })
{}


database::~database()
{}

std::unique_ptr<sql>
database::make_sql() const {
    return make_dialect_sql();
}

void
database::create_schema(const std::string &schema_name) const {
    const unique_ptr<dialect_sql> cmd = make_dialect_sql();
    cmd->write_create_schema(schema_name);
    make_schemaless_session()->exec(*cmd);
}

bool
database::create_schema_if_not_exists(const optional<string> &schema_name) const {
    if (! schema_name  ||   _named_schemas_known_to_exist.count(*schema_name))
        return false;

    const unique_ptr<dialect_sql> cmd = make_dialect_sql();
    cmd->write_create_schema(*schema_name);
    const bool result = make_schemaless_session()->unchecked_exec(*cmd);
    _named_schemas_known_to_exist.insert(*schema_name);
    return result;
}

optional<std::string>
database::get_default_enclosure() const {
    return _spec._default_schema;
}

void
database::make_enclosure_available(const optional<string> &enclosure_name) const {
    create_schema_if_not_exists(enclosure_name);
}

unique_ptr<session_impl>
database::make_schemaless_session() const {
    session_impl::spec s = _spec;
    s._default_schema = boost::none;
    return make_unique<session_impl>(*this, s);
}

new_session
database::make_session() const {
    new_session result = make_unique<session_impl>(*this, _spec);
    if (const optional<string> default_schema = get_default_enclosure()) {
        const unique_ptr<sql> cmd = make_sql();
        cmd->write_set_search_path(*default_schema);
        result->exec(*cmd);
    }
    return result;
}

vector<string>
database::retrieve_column_titles(const binomen &table) const {
    const unique_ptr<sql> cmd = make_sql();
    cmd->write_select_none(table);
    return get_session_impl()->exec_with_metadata_output(*cmd);
}

serial
database::insert_with_readback(unique_ptr<sql> insert, const serial_mapper &readback_mapper) const {
    insert->write_returning(readback_mapper);
    unique_ptr<row> output = get_session_impl()->exec_with_one_output(*insert);
    if (! output)  throw no_row_exception();

    serial result;
    readback_mapper.from_row(*output, result);
    return result;
}

string
database::column_type_name(column_type type) const {
    switch (type)   {
        case column_type::boolean:          return "boolean";
        case column_type::small_int:        return "smallint";
        case column_type::integer:          return "integer";
        case column_type::big_int:          return "bigint";
        case column_type::big_serial:       return "bigserial";
        case column_type::floating_point:   return "real";
        case column_type::double_precision: return "double precision";
        case column_type::string:           return "text";
        case column_type::byte_vector:      return "bytea";
        default:                            abort();
    }
}

shared_ptr<session_impl>
database::get_session_impl() const {
    return dynamic_pointer_cast<session_impl>(get_session());
}

unique_ptr<dialect_sql>
database::make_dialect_sql() const {
    return make_unique<dialect_sql>(*this);
}

column_type
database::retrievable_column_type(column_type declared) const {
    if (declared == column_type::big_serial)
        return column_type::big_int;
    return declared;
}

optional<size_t>
database::max_column_name_length() const {
    return NAMEDATALEN;
}

}
