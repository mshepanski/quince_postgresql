#ifndef QUINCE_POSTGRESQL__detail__session_h
#define QUINCE_POSTGRESQL__detail__session_h

//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../../../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

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
        boost::optional<std::string> _host;
        boost::optional<std::string> _user;
        boost::optional<std::string> _password;
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
