//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <pg_config_manual.h>  // for NAMEDATALEN
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <date/tz.h>
#include <quince/exceptions.h>
#include <quince/detail/compiler_specific.h>
#include <quince/detail/session.h>
#include <quince/detail/util.h>
#include <quince/transaction.h>
#include <quince/mappers/direct_mapper.h>
#include <quince/mappers/numeric_cast_mapper.h>
#include <quince/mappers/reinterpret_cast_mapper.h>
#include <quince/mappers/detail/abstract_mapper.h>
#include <quince/mappers/serial_mapper.h>
#include <quince_postgresql/database.h>
#include <quince_postgresql/detail/dialect_sql.h>
#include <sstream>
#include <chrono>
#include <vector>


using boost::optional;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::multiprecision::cpp_dec_float_100;
using date::zoned_time;
using namespace quince;
using std::dynamic_pointer_cast;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;


QUINCE_SUPPRESS_MSVC_DOMINANCE_WARNING

namespace quince_postgresql {

namespace {
    class ptime_mapper : public abstract_mapper<ptime>, public direct_mapper<timestamp>
    {
    public:
        explicit ptime_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<ptime>(name),
            direct_mapper<timestamp>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<ptime_mapper>(*this);
        }

        virtual void from_row(const row &src, ptime &dest) const override {
            timestamp text;
            direct_mapper<timestamp>::from_row(src, text);
            dest = boost::posix_time::time_from_string(text);
        }

        virtual void to_row(const ptime &src, row &dest) const override {
            const timestamp text(boost::posix_time::to_simple_string(src));
            direct_mapper<timestamp>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<ptime>::build_match_tester(qb, result);
        }
    };

    class time_mapper : public abstract_mapper<time_duration>, public direct_mapper<time_type>
    {
    public:
        explicit time_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<time_duration>(name),
            direct_mapper<time_type>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<time_mapper>(*this);
        }

        virtual void from_row(const row &src, time_duration &dest) const override {
            time_type text;
            direct_mapper<time_type>::from_row(src, text);
            dest = boost::posix_time::duration_from_string(text);
        }

        virtual void to_row(const time_duration &src, row &dest) const override {
            const time_type text(boost::posix_time::to_simple_string(src));
            direct_mapper<time_type>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<time_duration>::build_match_tester(qb, result);
        }
    };

    class date_mapper : public abstract_mapper<boost::gregorian::date>, public direct_mapper<date_type>
    {
    public:
        explicit date_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<boost::gregorian::date>(name),
            direct_mapper<date_type>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<date_mapper>(*this);
        }

        virtual void from_row(const row &src, boost::gregorian::date&dest) const override {
            date_type text;
            direct_mapper<date_type>::from_row(src, text);
            dest = boost::gregorian::from_string(text);
        }

        virtual void to_row(const boost::gregorian::date &src, row &dest) const override {
            const date_type text(boost::gregorian::to_simple_string(src));
            direct_mapper<date_type>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<boost::gregorian::date>::build_match_tester(qb, result);
        }
    };

    class numeric_mapper : public abstract_mapper<cpp_dec_float_100>, public direct_mapper<numeric_type>
    {
    public:
        explicit numeric_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<cpp_dec_float_100>(name),
            direct_mapper<numeric_type>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<numeric_mapper>(*this);
        }

        virtual void from_row(const row &src, cpp_dec_float_100&dest) const override {
            numeric_type text;
            direct_mapper<numeric_type>::from_row(src, text);
            dest = cpp_dec_float_100(static_cast<const std::string&>(text));
        }

        virtual void to_row(const cpp_dec_float_100 &src, row &dest) const override {
            const numeric_type text(src.str(std::numeric_limits<cpp_dec_float_100>::digits10));
            direct_mapper<numeric_type>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<cpp_dec_float_100>::build_match_tester(qb, result);
        }
    };

    template <typename DurationT>
    class timestamp_with_tz_mapper : public abstract_mapper<zoned_time<DurationT>>, public direct_mapper<timestamp_with_tz>
    {
    public:
        explicit timestamp_with_tz_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<zoned_time<DurationT>>(name),
            direct_mapper<timestamp_with_tz>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<timestamp_with_tz_mapper>(*this);
        }

        virtual void from_row(const row &src, zoned_time<DurationT> &dest) const override {
            timestamp_with_tz text;
            direct_mapper<timestamp_with_tz>::from_row(src, text);
            std::stringstream ss;
            ss << std::string(text);
            std::chrono::sys_time<DurationT> time_point;
            ss >> date::parse("%F %T", time_point);
            dest = date::make_zoned(time_point);
        }

        virtual void to_row(const zoned_time<DurationT>& src, row &dest) const override {
            timestamp_with_tz text;
            std::stringstream ss;
            ss << date::format("%F %T %z", src);
            text = ss.str();
            direct_mapper<timestamp_with_tz>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<zoned_time<DurationT>>::build_match_tester(qb, result);
        }
    };

    template <typename IntT, typename ArrayT>
    class array_of_int_mapper : public abstract_mapper<std::vector<IntT>>, public direct_mapper<ArrayT>
    {
    public:
        explicit array_of_int_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<std::vector<IntT>>(name),
            direct_mapper<ArrayT>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<array_of_int_mapper<IntT,ArrayT>>(*this);
        }

        virtual void from_row(const row &src, std::vector<IntT> &dest) const override {
            ArrayT text;
            direct_mapper<ArrayT>::from_row(src, text);
            std::stringstream ss(std::string(text).substr(1, std::string(text).size() -2));
            if(!ss.str().empty()) {
                IntT num;
                while(ss >> num) {
                    dest.push_back(num);
                    ss.ignore(1, ',');
                }
            }
        }

        virtual void to_row(const std::vector<IntT>& src, row &dest) const override {
            ArrayT text;
            std::stringstream ss;
            ss << "{";
            if (!src.empty()) {
                std::copy(std::begin(src), std::end(src)-1, std::ostream_iterator<IntT>(ss, ","));
                ss << src.back();
            }
            ss << "}";
            text = ss.str();
            direct_mapper<ArrayT>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<std::vector<IntT>>::build_match_tester(qb, result);
        }
    };

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
            customize<ptime, ptime_mapper>();
            customize<time_duration, time_mapper>();
            customize<boost::gregorian::date, date_mapper>();
            customize<json_type, direct_mapper<json_type>>();
            customize<jsonb_type, direct_mapper<jsonb_type>>();
            customize<cpp_dec_float_100, numeric_mapper>();
            customize<zoned_time<std::chrono::milliseconds>, timestamp_with_tz_mapper<std::chrono::milliseconds>>();
            customize<zoned_time<std::chrono::microseconds>, timestamp_with_tz_mapper<std::chrono::microseconds>>();
            customize<std::vector<std::int16_t>, array_of_int_mapper<std::int16_t, array_of_int16>>();
            customize<std::vector<std::int32_t>, array_of_int_mapper<std::int32_t, array_of_int32>>();
            customize<std::vector<std::int64_t>, array_of_int_mapper<std::int64_t, array_of_int64>>();
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
        quince::make_unique<customization_for_dbms>()
    ),
    _spec({
        to_optional(host),
        to_optional(user),
        to_optional(password),
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
    return quince::make_unique<session_impl>(*this, s);
}

new_session
database::make_session() const {
    new_session result = quince::make_unique<session_impl>(*this, _spec);
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
        case column_type::boolean:              return "boolean";
        case column_type::small_int:            return "smallint";
        case column_type::integer:              return "integer";
        case column_type::big_int:              return "bigint";
        case column_type::big_serial:           return "bigserial";
        case column_type::floating_point:       return "real";
        case column_type::double_precision:     return "double precision";
        case column_type::string:               return "text";
        case column_type::timestamp:            return "timestamp";
        case column_type::time_type:            return "time";
        case column_type::date_type:            return "date";
        case column_type::json_type:            return "json";
        case column_type::jsonb_type:           return "jsonb";
        case column_type::numeric_type:         return "numeric";
        case column_type::byte_vector:          return "bytea";
        case column_type::timestamp_with_tz:    return "timestamp with time zone";
        case column_type::array_of_int16:       return "smallint[]";
        case column_type::array_of_int32:       return "integer[]";
        case column_type::array_of_int64:       return "bigint[]";
        default:                                abort();
    }
}

shared_ptr<session_impl>
database::get_session_impl() const {
    return dynamic_pointer_cast<session_impl>(get_session());
}

unique_ptr<dialect_sql>
database::make_dialect_sql() const {
    return quince::make_unique<dialect_sql>(*this);
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

QUINCE_UNSUPPRESS_MSVC_WARNING
