//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <assert.h>
#include <queue>
#include <string>
#include <sstream>
#include <vector>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/utility/identity_type.hpp>
#include <libpq/libpq-fs.h>
#include <quince/detail/column_type.h>
#include <quince/exceptions.h>
#include <quince/detail/row.h>
#include <quince/detail/util.h>
#include <quince_postgresql/database.h>
#include <quince_postgresql/detail/dialect_sql.h>
#include <quince_postgresql/detail/session.h>

using boost::format;
using boost::optional;
using namespace quince;
using std::dynamic_pointer_cast;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;


#define BOOLOID 16
#define BYTEAOID 17
#define INT8OID 20
#define INT2OID 21
#define INT4OID 23
#define TEXTOID 25
#define OIDOID 26
#define FLOAT4OID 700
#define FLOAT8OID 701
#define DATEOID 1082
#define JSONOID 114
#define JSONBOID 3802
#define TIMEOID 1083
#define TIMESTAMPOID 1114
#define VOIDOID 2278
#define TSVECTOROID 3614
#define UNKNOWNOID 705
#define NUMERICOID 1700
#define TIMESTAMPTZOID 1184


namespace quince_postgresql {

namespace {
    Oid
    standard_type_oid(column_type type) {
        switch (type) {
            case column_type::boolean:              return BOOLOID;
            case column_type::small_int:            return INT2OID;
            case column_type::integer:              return INT4OID;
            case column_type::big_int:              return INT8OID;
            case column_type::floating_point:       return FLOAT4OID;
            case column_type::double_precision:     return FLOAT8OID;
            case column_type::date_type:            return DATEOID;
            case column_type::json_type:            return JSONOID;
            case column_type::jsonb_type:           return JSONBOID;
            case column_type::time_type:            return TIMEOID;
            case column_type::timestamp:            return TIMESTAMPOID;
            case column_type::string:               return TEXTOID;
            case column_type::numeric_type:         return NUMERICOID;
            case column_type::byte_vector:          return BYTEAOID;
            case column_type::timestamp_with_tz:    return TIMESTAMPTZOID;
            case column_type::none:                 return VOIDOID;
            default:                            abort();
        }
    }

    column_type
    get_column_type(Oid type_oid)  {
        switch (type_oid) {
            case BOOLOID:           return column_type::boolean;
            case INT2OID:           return column_type::small_int;
            case INT4OID:           return column_type::integer;
            case INT8OID:           return column_type::big_int;
            case FLOAT4OID:         return column_type::floating_point;
            case FLOAT8OID:         return column_type::double_precision;
            case DATEOID:           return column_type::date_type;
            case JSONOID:           return column_type::json_type;
            case JSONBOID:          return column_type::jsonb_type;
            case TIMEOID:           return column_type::time_type;
            case TIMESTAMPOID:      return column_type::timestamp;
            case TEXTOID:           return column_type::string;
            case NUMERICOID:        return column_type::numeric_type;
            case BYTEAOID:          return column_type::byte_vector;
            case TIMESTAMPTZOID:    return column_type::timestamp_with_tz; 
            case VOIDOID:           return column_type::none;
            default:                throw retrieved_unrecognized_type_exception(type_oid);
        }
    }


    class exec_params {
    public:
        explicit exec_params(const vector<cell> &data) :
            _n_params(data.size()),
            _types(new Oid[_n_params]),
            _values(new const char *[_n_params]),
            _lengths(new int[_n_params]),
            _formats(new int[_n_params])
        {
            for (size_t i = 0; i < _n_params; i++) {
                const cell &c = data[i];
                if (c.type() == column_type::none) {
                    _types[i] = 0;
                    _values[i] = NULL;
                    _lengths[i] = 0;
                }
                else {
                    _types[i] = standard_type_oid(c.type());
                    _values[i] = static_cast<const char *>(c.data());
                    _lengths[i] = boost::numeric_cast<int>(c.size());
                }
                _formats[i] = 1;  // always binary
            }
        }

        PGresult *
        exec(PGconn * const conn, const string &sql) const {
            return PQexecParams(
                conn,
                sql.c_str(),
                boost::numeric_cast<int>(_n_params),
                _types.get(),
                _values.get(),
                _lengths.get(),
                _formats.get(),
                1
            );
        };

        int
        send(PGconn * const conn, const string &sql) const {
            return PQsendQueryParams(
                conn,
                sql.c_str(),
                boost::numeric_cast<int>(_n_params),
                _types.get(),
                _values.get(),
                _lengths.get(),
                _formats.get(),
                1
            );
        }

    private:
        const size_t _n_params;
        std::unique_ptr<Oid[]> _types;
        std::unique_ptr<BOOST_IDENTITY_TYPE((const char *))[]> _values;
        std::unique_ptr<int[]> _lengths;
        const std::unique_ptr<int[]> _formats;
    };

    string
    new_cursor_name() {
        static uint64_t count = 0;
        return "cursor_" + std::to_string(count++);
    }

    class query_result {
    public:
        explicit query_result(const database &database, PGresult *pg_result) :
            _database(database),
            _pg_result(pg_result),
            _n_rows(boost::numeric_cast<uint32_t>(PQntuples(pg_result))),
            _n_cols(boost::numeric_cast<uint32_t>(PQnfields(pg_result))),
            _col_names(new string[_n_cols]),
            _type_oids(new Oid[_n_cols]),
            _current_row(0)
        {
            for (uint32_t i = 0; i < _n_cols; i++) {
                const char *chars = PQfname(_pg_result, i);
                if (chars == NULL)  throw malformed_results_exception();
                _col_names[i] = chars;
                _type_oids[i] = PQftype(_pg_result, i);
            }
        }

        ~query_result() {
            if (_pg_result != NULL)  PQclear(_pg_result);
        }

        vector<string>
        metadata() const {
            vector<string> result;
            result.reserve(_n_cols);
            for (uint32_t i = 0; i < _n_cols; i++) {
                const string &col_name = _col_names[i];
                const string type_name = _database.column_type_name(get_column_type(_type_oids[i]));
                result.push_back((format("\"%1%\" %2%") % col_name % type_name).str());
            }
            return result;
        }
    
        bool
        bad_no_data() const {
            return PQresultStatus(_pg_result) != PGRES_COMMAND_OK;
        }

        bool
        bad_data() const {
            return PQresultStatus(_pg_result) != PGRES_TUPLES_OK;
        }

        bool
        at_end() const {
            return _current_row == _n_rows;
        }

        unique_ptr<row>
        next() {
            if (at_end())  return nullptr;

            unique_ptr<row> result = quince::make_unique<row>(&_database);

            for (uint32_t i = 0; i < _n_cols; i++) {
                const optional<column_type> col_type(
                    ! PQgetisnull(_pg_result, _current_row, i),
                    get_column_type(_type_oids[i])
                );
                const cell cell(
                    col_type,
                    PQfformat(_pg_result, i) == 1,
                    PQgetvalue(_pg_result, _current_row, i),
                    boost::numeric_cast<size_t>(PQgetlength(_pg_result, _current_row, i))
                );
                result->add_cell(cell, _col_names[i]);
            }
            _current_row++;
            return result;
        }

    private:
        const database &_database;
        PGresult *const _pg_result;
        const uint32_t _n_rows;
        const uint32_t _n_cols;
        std::unique_ptr<string[]> _col_names;
        std::unique_ptr<Oid[]> _type_oids;
        uint32_t _current_row;
    };
}

class session_impl::result_stream_impl : public abstract_result_stream_impl {
public:
    result_stream_impl(
        const database &database,
        const string &cursor_name,
        PGconn *conn,
        uint32_t fetch_size,
        const std::function<int(const sql &)> send,
        const std::function<void(void)> epilogue
        ) :
        _database(database),
        _sql_fetch(database.make_dialect_sql()),
        _conn(conn),
        _send(send),
        _epilogue(epilogue),
        _current(quince::make_unique<query_result>(_database, fetch())),
        _exhausted(false)
    {
        const_cast<dialect_sql &>(*_sql_fetch).write_fetch(cursor_name, fetch_size);
    }

    ~result_stream_impl() {
        try {
            close();
        }
        catch (...) {}
    }

    void
    close() {
        while (!_backlog.empty())  PQclear(take_from_backlog());
        _epilogue();
    }

    void
    absorb() {
        while (PGresult *r = PQgetResult(_conn))  _backlog.push(r);
    }

    unique_ptr<row>
    next() {
        unique_ptr<row> result;
        for (;;) {
            if (_exhausted)
                return nullptr;
            else if (result)
                return result;
            else if (_current && !_current->at_end())
                result = _current->next();
            else if (! _backlog.empty())
                _current = quince::make_unique<query_result>(_database, take_from_backlog());
            else if (PGresult *gotten = PQgetResult(_conn))
                _backlog.push(gotten);
            else if (PGresult *fetched = fetch())
                _backlog.push(fetched);
            else
                _exhausted = true;
        }
    }

private:
    PGresult *
    fetch() {
        _send(*_sql_fetch);
        while (PGresult * const r = PQgetResult(_conn)) {
            if (PQntuples(r) == 0)
                PQclear(r);
            else
                return r;
        }
        return nullptr;
    }

    PGresult *
    take_from_backlog() {
        PGresult * const result = _backlog.front();
        _backlog.pop();
        return result;
    }

    const database &_database;
    unique_ptr<const dialect_sql> _sql_fetch;
    PGconn * const _conn;
    const std::function<int(const sql &)> _send;
    const std::function<void(void)> _epilogue;
    unique_ptr<query_result> _current;
    bool _exhausted;
    std::queue<PGresult *> _backlog;
};


string
session_impl::spec::connection_string() const {
    stringstream strm;
    if (_host)
        strm << " host=" << *_host;
    if (_user)
        strm << " user=" << *_user;
    if (_password)
        strm << " password=" << *_password;
    if (_port)
        strm << " port=" << *_port;
    if (_db_name)
        strm << " dbname=" << *_db_name;
    string str = strm.str();
    return str.empty() ? str : str.substr(1);
}

extern "C" {
    void ignore_postgresql_notice(void *a_arg, const PGresult *a_res)  {}
}

session_impl::session_impl(const database &database, const session_impl::spec &spec) :
    _database(database),
    _conn(connect(spec))
{
    if (! _conn  ||  PQstatus(_conn) != CONNECTION_OK)
        throw failed_connection_exception();
    if (spec._isolation) {
        unique_ptr<dialect_sql> cmd = _database.make_dialect_sql();
        cmd->write_set_session_characteristics(*spec._isolation);
        exec(*cmd);
    }
}

session_impl::~session_impl() {
    _asynchronous_stream.reset();
    if (_conn)  disconnect(_conn);
}

void
session_impl::ignore_notices() {
    absorb_pending_results();
    PQsetNoticeReceiver(_conn, ignore_postgresql_notice, nullptr);
}

bool
session_impl::unchecked_exec(const sql &cmd) {
    assert(! _asynchronous_stream);
    return ! query_result(_database, pq_exec(cmd)).bad_no_data();
}

unique_ptr<row>
session_impl::exec_with_one_output(const sql &cmd) {
    absorb_pending_results();
    return one_output(pq_exec(cmd));
}

result_stream
session_impl::exec_with_stream_output(const sql &cmd, uint32_t fetch_size) {
    absorb_pending_results();
    const string cursor_name = new_cursor_name();
    const unique_ptr<dialect_sql> declare = clone(dynamic_cast<const dialect_sql &>(cmd));
    declare->prepend_declare_cursor(cursor_name);
    check_no_output(pq_exec(*declare));
    return new_result_stream(cursor_name, fetch_size);
}

vector<string>
session_impl::exec_with_metadata_output(const sql &cmd) {
    return metadata(pq_exec(cmd));
}

void
session_impl::exec(const sql &cmd) {
    absorb_pending_results();
    check_no_output(pq_exec(cmd));
}

std::uint64_t
session_impl::exec_with_count_output(const sql &cmd) {
    absorb_pending_results();
    PGresult* result = pq_exec(cmd);
    char* tuples = PQcmdTuples(result);
    std::uint64_t count = (tuples && *tuples) ? strtoull(tuples, nullptr, 10) : 0;
    check_no_output(result);
    return count;
}

unique_ptr<row>
session_impl::next_output(const result_stream &rs) {
    assert(rs);
    shared_ptr<result_stream_impl> rsi = dynamic_pointer_cast<result_stream_impl>(rs);
    assert(rsi);
    if (rsi != _asynchronous_stream) {
        absorb_pending_results();
        assert(! _asynchronous_stream);
        _asynchronous_stream = rsi;
    }
    return rsi->next();
}

string
session_impl::encoding() const {
    return PQparameterStatus(_conn, "server_encoding");
}

void
session_impl::throw_last_error() const {
    const char *const dbms_message = PQerrorMessage(_conn);
    string message(dbms_message ? dbms_message : "");
    const enum { deadlock, broken_connection, other } category =
          message.find("ERROR:  deadlock detected") == 0?
            deadlock
        : message.find("ERROR:  could not serialize access due to concurrent update") == 0?
            deadlock
        : message.find("server closed the connection unexpectedly") == 0?
            broken_connection
        : message.find("no connection to the server") == 0?
            broken_connection
        :
            other;
    message += " (most recent SQL command was `" + _latest_sql + "')";

    switch (category) {
        case deadlock:          throw deadlock_exception(message);
        case broken_connection: _database.discard_connections();
                                throw broken_connection_exception(message);
        default:                throw dbms_exception(message);
    }
}

void
session_impl::check_no_output(PGresult *exec_result) {
    if (query_result(_database, exec_result).bad_no_data())  throw_last_error();
}

unique_ptr<row>
session_impl::one_output(PGresult *exec_result) {
    query_result r(_database, exec_result);
    if (r.bad_data())  throw_last_error();

    unique_ptr<row> row = r.next();
    if (! r.at_end())  throw multi_row_exception();

    return row;
}

vector<string>
session_impl::metadata(PGresult *exec_result) {
    const query_result r(_database, exec_result);
    if (r.bad_data())  throw_last_error();
    return r.metadata();
}

void
session_impl::absorb_pending_results() {
    if (_asynchronous_stream) {
        _asynchronous_stream->absorb();
        _asynchronous_stream.reset();
    }
    else if (PGresult *pg_result = PQgetResult(_conn)) {
        // unexpected residual results.
        do PQclear(pg_result);
        while ((pg_result = PQgetResult(_conn)) != nullptr);
    }
}

PGresult *
session_impl::pq_exec(const sql &cmd) {
    return exec_params(cmd.get_input().values()).exec(
        _conn,
        _latest_sql = cmd.get_text()
    );
}

int
session_impl::pq_send(const sql &cmd) {
    return exec_params(cmd.get_input().values()).send(
        _conn,
        _latest_sql = cmd.get_text()
    );
}

result_stream
session_impl::new_result_stream(const string &cursor_name, uint32_t fetch_size) {
    assert(!_asynchronous_stream);

    _asynchronous_stream = quince::make_unique<result_stream_impl>(
        _database,
        cursor_name,
        _conn,
        fetch_size,
        [this] (const sql &cmd) { return pq_send(cmd); },
        [this, cursor_name]     { close_cursor(cursor_name); }
    );
    return _asynchronous_stream;
}

void
session_impl::close_cursor(const string &cursor_name) {
    const unique_ptr<dialect_sql> cmd = _database.make_dialect_sql();
    cmd->write_close_cursor(cursor_name);
    check_no_output(pq_exec(*cmd));
}

PGconn *
session_impl::connect(const session_impl::spec &spec) {
    assert(! _disabled);
    if (! _have_registered_disabler) {
        // We might get here more than once, because of a race condition.  No harm done.
        //
        atexit(disable);
        // Avoid PQfinish() calls after exit(), because (a) there's no benefit and more
        // importantly (b) they can crash because the libpq might have shut down by then.
        //
        _have_registered_disabler = false;
    }
    return PQconnectdb(spec.connection_string().c_str());
}

void
session_impl::disconnect(PGconn *conn) {
    if (! _disabled)  PQfinish(conn);
}

void
session_impl::disable() {
    _disabled = true;
}

bool session_impl::_disabled = false;  // I can rely on load-time initialization (i.e. before the static init sequence), because bool is a POD.
bool session_impl::_have_registered_disabler = false;

}
