#ifndef QUINCE_POSTGRESQL__detail__session_h
#define QUINCE_POSTGRESQL__detail__session_h

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

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <libpq-fe.h>
#include <quince/detail/compiler_specific.h>
#include <quince/detail/session.h>


namespace quince_postgresql {

class database;

enum class isolation_level {
    serializable, repeatable_read, read_committed, read_uncommitted
};

class session_impl : public quince::abstract_session_impl {
public:
    struct spec {
        std::string _host;
        std::string _user;
        std::string _password;
        boost::optional<std::string> _db_name;
        boost::optional<std::string> _default_schema;
        boost::optional<std::string> _port;
        boost::optional<isolation_level> _isolation;

        std::string connection_string() const;
    };

    explicit session_impl(const database &database, const session_impl::spec &spec);

    virtual ~session_impl();

    virtual bool                            unchecked_exec(const quince::sql &) override;
    virtual void                            exec(const quince::sql &) override;
    virtual quince::result_stream           exec_with_stream_output(const quince::sql &, uint32_t fetch_size) override;
    virtual std::unique_ptr<quince::row>    exec_with_one_output(const quince::sql &) override;
    virtual std::unique_ptr<quince::row>    next_output(const quince::result_stream &) override;

    std::vector<std::string> exec_with_metadata_output(const quince::sql &cmd);

    void ignore_notices();

    std::string encoding() const;

private:
    class result_stream_impl;

    QUINCE_NORETURN void throw_last_error() const;

    void check_no_output(PGresult *exec_result);

    std::unique_ptr<quince::row> one_output(PGresult *exec_result);

    std::vector<std::string> metadata(PGresult *exec_result);

    void absorb_pending_results();

    PGresult *pq_exec(const quince::sql &cmd);

    int pq_send(const quince::sql &cmd);

    quince::result_stream new_result_stream(const std::string &cursor_name, uint32_t fetch_size);

    void close_cursor(const std::string &cursor_name);

    static PGconn *connect(const spec &);
    static void disconnect(PGconn *);
    static void disable();

    const database &_database;
    PGconn * const _conn;
    std::shared_ptr<result_stream_impl> _asynchronous_stream;
    std::string _latest_sql;

    static bool _disabled;
    static bool _have_registered_disabler;
};

}

#endif
