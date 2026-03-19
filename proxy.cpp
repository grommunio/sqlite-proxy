// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 grommunio GmbH
// This file is part of sqlite-proxy
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <spawn.h>
#include <sqlite3.h>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/wait.h>
#include <libHX/scope.hpp>
#include <libHX/string.h>
#include "proxy.hpp"
#define GX_EXPORT __attribute__((visibility("default")))
#define POPENFD_NULL (nullptr)
#define POPENFD_KEEP (&popenfd_keepfd_marker)

enum {
	SQLITE_DEADCONN = 96,
	SQLITE_PKTERR = 97,
};

static int popenfd_keepfd_marker;

struct popen_fdset {
	int in[2] = {-1, -1}, out[2] = {-1, -1}, err[2] = {-1, -1}, null = -1;

	popen_fdset() = default;
	~popen_fdset()
	{
		if (in[0] != -1) close(in[0]);
		if (in[1] != -1) close(in[1]);
		if (out[0] != -1) close(out[0]);
		if (out[1] != -1) close(out[1]);
		if (err[0] != -1) close(err[0]);
		if (err[1] != -1) close(err[1]);
		if (null != -1) close(null);
	}
	popen_fdset(popen_fdset &&) = delete;
	void operator=(popen_fdset &&) = delete;
};

static pid_t popenfd(const char *prog, const char *const *argv, int *fdinp,
    int *fdoutp, int *fderrp, const char *const *env)
{
	if (argv == nullptr || argv[0] == nullptr)
		return -EINVAL;

	popen_fdset fd;
	if (fdinp == nullptr || fdoutp == nullptr || fderrp == nullptr) {
		fd.null = ::open("/dev/null", O_RDWR);
		if (fd.null < 0)
			return -errno;
	}
	posix_spawn_file_actions_t fa{};
	auto ret = posix_spawn_file_actions_init(&fa);
	if (ret != 0)
		return -ret;
	auto cl2 = HX::make_scope_exit([&]() { posix_spawn_file_actions_destroy(&fa); });

	/* Close child-unused ends of the pipes; move child-used ends to fd 0-2. */
	if (fdinp == POPENFD_NULL || fdinp == POPENFD_KEEP) {
		ret = posix_spawn_file_actions_adddup2(&fa, fd.null, STDIN_FILENO);
		if (ret != 0)
			return -ret;
	} else {
		if (::pipe(fd.in) < 0)
			return -errno;
		ret = posix_spawn_file_actions_addclose(&fa, fd.in[1]);
		if (ret != 0)
			return -ret;
		ret = posix_spawn_file_actions_adddup2(&fa, fd.in[0], STDIN_FILENO);
		if (ret != 0)
			return -ret;
	}

	if (fdoutp == POPENFD_NULL) {
		ret = posix_spawn_file_actions_adddup2(&fa, fd.null, STDOUT_FILENO);
		if (ret != 0)
			return -ret;
	} else if (fdoutp == POPENFD_KEEP) {
	} else {
		if (::pipe(fd.out) < 0)
			return -errno;
		ret = posix_spawn_file_actions_addclose(&fa, fd.out[0]);
		if (ret != 0)
			return -ret;
		ret = posix_spawn_file_actions_adddup2(&fa, fd.out[1], STDOUT_FILENO);
		if (ret != 0)
			return -ret;
	}

	if (fderrp == POPENFD_NULL) {
		ret = posix_spawn_file_actions_adddup2(&fa, fd.null, STDERR_FILENO);
		if (ret != 0)
			return -ret;
	} else if (fderrp == POPENFD_KEEP) {
	} else if (fderrp == fdoutp) {
		ret = posix_spawn_file_actions_adddup2(&fa, fd.out[1], STDERR_FILENO);
		if (ret != 0)
			return -ret;
	} else {
		if (fderrp != nullptr && fderrp != fdoutp && ::pipe(fd.err) < 0)
			return -errno;
		ret = posix_spawn_file_actions_addclose(&fa, fd.err[0]);
		if (ret != 0)
			return -ret;
		ret = posix_spawn_file_actions_adddup2(&fa, fd.err[1], STDERR_FILENO);
		if (ret != 0)
			return -ret;
	}

	/* Close all pipe ends that were not already fd 0-2. */
	if (fd.in[0] != -1 && fd.in[0] != STDIN_FILENO &&
	    (ret = posix_spawn_file_actions_addclose(&fa, fd.in[0])) != 0)
		return -ret;
	if (fderrp != fdoutp) {
		if (fd.out[1] != -1 && fd.out[1] != STDOUT_FILENO &&
		    (ret = posix_spawn_file_actions_addclose(&fa, fd.out[1])) != 0)
			return -ret;
		if (fd.err[1] != -1 && fd.err[1] != STDERR_FILENO &&
		    (ret = posix_spawn_file_actions_addclose(&fa, fd.err[1])) != 0)
			return -ret;
	} else {
		if (fd.out[1] != -1 && fd.out[1] != STDOUT_FILENO &&
		    fd.out[1] != STDERR_FILENO &&
		    (ret = posix_spawn_file_actions_addclose(&fa, fd.out[1])) != 0)
			return -ret;
	}
	if (fd.null != -1 && fd.null != STDIN_FILENO &&
	    fd.null != STDOUT_FILENO && fd.null != STDERR_FILENO &&
	    (ret = posix_spawn_file_actions_addclose(&fa, fd.null)) != 0)
		return -ret;

	pid_t pid = -1;
	ret = posix_spawnp(&pid, prog, &fa, nullptr,
	      const_cast<char **>(argv), const_cast<char **>(env));
	if (ret != 0)
		return -ret;
	if (fdinp != nullptr && fdinp != POPENFD_KEEP) {
		*fdinp = fd.in[1];
		fd.in[1] = -1;
	}
	if (fdoutp != nullptr && fdoutp != POPENFD_KEEP) {
		*fdoutp = fd.out[0];
		fd.out[0] = -1;
	}
	if (fderrp != nullptr && fderrp != fdoutp && fderrp != POPENFD_KEEP) {
		*fderrp = fd.err[0];
		fd.err[0] = -1;
	}
	return pid;
}

/*
 * write() might issue SIGPIPE; the surrounding process should have it ignored.
 */
static bool wire_write_all(int fd, const void *buf, size_t len)
{
	auto p = static_cast<const uint8_t *>(buf);
	while (len > 0) {
		ssize_t n = write(fd, p, len);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return false;
		}
		p += n;
		len -= static_cast<size_t>(n);
	}
	return true;
}

static bool wire_read_all(int fd, void *buf, size_t len)
{
	auto p = static_cast<uint8_t *>(buf);
	while (len > 0) {
		ssize_t n = read(fd, p, len);
		if (n <= 0) {
			if (n < 0 && errno == EINTR)
				continue;
			return false;
		}
		p += n;
		len -= static_cast<size_t>(n);
	}
	return true;
}

struct sqlite3i;

struct sqlite3_stmt {
	public:
	uint64_t remote_id = 0;
	std::weak_ptr<sqlite3i> db;
	sqlite3 *rdb = nullptr;
	std::string sql_text;

	class col_data {
		public:
		void clear() {
			type = SQLITE_NULL;
			i64 = 0;
			dbl = 0.0;
			blob.clear();
			text_converted = false;
		}

		int type = SQLITE_NULL;
		int64_t i64 = 0;
		double dbl = 0.0;
		std::string blob;
		bool text_converted = false;
	};
	std::vector<col_data> columns;
};

struct sqlite3i : public std::enable_shared_from_this<sqlite3i> {
	~sqlite3i();

	int read_fd = -1, write_fd = -1;
	pid_t child_pid = -1;
	uint64_t next_stmt_id = 1;
	std::string errmsg, db_filename;
	std::mutex mtx;
	std::unordered_map<uint64_t, std::unique_ptr<sqlite3_stmt>> stmts;
	/* Reusable IPC buffers -- avoids heap alloc/free per RPC call */
	msg_buf rpc_req, rpc_resp;
};

struct sqlite3 {
	std::shared_ptr<sqlite3i> m_impl;
};

sqlite3i::~sqlite3i()
{
	if (write_fd >= 0)
		close(write_fd);
	if (read_fd >= 0)
		close(read_fd);
	if (child_pid > 0) {
		int status = 0;
		if (waitpid(child_pid, &status, WNOHANG) == 0)
			waitpid(child_pid, &status, 0);
	}
}

static bool rpc_call(sqlite3i &db)
{
	if (!db.rpc_req.send(db.write_fd))
		return false;
	return db.rpc_resp.recv(db.read_fd);
}

static const char *find_worker()
{
	const char *env = getenv("SQLITE_WORKER");
	return env != nullptr ? env : "SQLITE_WORKER-unset";
}

GX_EXPORT int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags,
    const char *zVfs) try
{
	if (ppDb == nullptr)
		return SQLITE_MISUSE;
	if (ppDb != nullptr)
		*ppDb = nullptr;

	auto db = std::make_unique<sqlite3>();
	/*
	 * We are sending the filename via argv just so that ps(1) can show it;
	 * the real filename is obtained via the RPCs.
	 */
	auto prog = find_worker();
	const char *args[] = {prog, HX_basename(prog), filename, nullptr};
	std::vector<const char *> new_env;
	for (char **evar = environ; *evar != nullptr; ++evar) {
		if (strncmp(*evar, "LD_PRELOAD=", 11) == 0 ||
		    strncmp(*evar, "LD_LIBRARY_PATH=", 16) == 0)
			continue;
		new_env.push_back(*evar);
	}
	new_env.push_back(nullptr);
	db->m_impl = std::make_shared<sqlite3i>();
	auto &dbi = *db->m_impl;
	dbi.child_pid = popenfd(args[0], &args[1], &dbi.write_fd, &dbi.read_fd,
	                POPENFD_KEEP, new_env.data());
	if (dbi.child_pid < 0)
		return SQLITE_CANTOPEN;

	auto &req  = dbi.rpc_req;
	auto &resp = dbi.rpc_resp;
	req.clear();
	req.put_u8(OP_OPEN_V2);
	req.put_str(filename);
	req.put_i32(flags);
	req.put_str(zVfs);
	if (!rpc_call(dbi))
		return SQLITE_CANTOPEN;

	int ret = resp.get_i32();
	const char *fn = resp.get_str();
	if (fn != nullptr)
		dbi.db_filename = fn;
	if (ret != SQLITE_OK)
		return ret;
	*ppDb = db.release();
	return SQLITE_OK;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_open(const char *filename, sqlite3 **ppDb)
{
	return sqlite3_open_v2(filename, ppDb,
	       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
}

/* testing function */
extern "C" int sqlite_proxy_close_nodel(sqlite3 *db);
GX_EXPORT int sqlite_proxy_close_nodel(sqlite3 *db)
{
	if (db == nullptr)
		return SQLITE_OK;
	auto &dbi = *db->m_impl;
	{
		std::lock_guard lk(dbi.mtx);
		auto &req = dbi.rpc_req;
		req.clear();
		req.put_u8(OP_CLOSE);
		if (!rpc_call(dbi))
			/* ignore */;
	}
	return SQLITE_OK;
}

GX_EXPORT int sqlite3_close_v2(sqlite3 *db)
{
	if (db == nullptr)
		return SQLITE_OK;
	auto &dbi = *db->m_impl;
	{
		std::lock_guard lk(dbi.mtx);
		auto &req = dbi.rpc_req;
		req.clear();
		req.put_u8(OP_CLOSE);
		if (!rpc_call(dbi))
			/* ignore */;
	}
	delete db;
	return SQLITE_OK;
}

GX_EXPORT int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
    sqlite3_stmt **ppStmt, const char **pzTail) try
{
	if (ppStmt)
		*ppStmt = nullptr;
	if (pzTail)
		*pzTail = nullptr;
	if (db == nullptr || zSql == nullptr)
		return SQLITE_MISUSE;

	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	uint64_t sid = dbi.next_stmt_id++;
	auto &req  = dbi.rpc_req;
	auto &resp = dbi.rpc_resp;
	req.clear();
	req.put_u8(OP_PREPARE_V2);
	req.put_str(zSql);
	req.put_i32(nByte);
	req.put_u64(sid);
	if (!rpc_call(dbi))
		return SQLITE_DEADCONN;

	int ret = resp.get_i32();
	uint8_t has = resp.get_u8();
	if (!has) {
		const char *err = resp.get_str();
		if (err != nullptr)
			dbi.errmsg = err;
		else
			dbi.errmsg.clear();
		return ret;
	}

	const char *st = resp.get_str();
	auto ps = std::make_unique<sqlite3_stmt>();
	ps->remote_id = sid;
	ps->db        = db->m_impl;
	ps->rdb       = db;
	ps->sql_text  = st != nullptr ? st : "";
	if (ppStmt != nullptr)
		*ppStmt = ps.release();
	dbi.stmts[sid] = std::move(ps);
	return ret;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_step(sqlite3_stmt *ps) try
{
	if (ps == nullptr)
		return SQLITE_MISUSE;
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DONE;

	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req  = dbi.rpc_req;
	auto &resp = dbi.rpc_resp;
	req.clear();
	req.put_u8(OP_STEP);
	req.put_u64(ps->remote_id);
	if (!rpc_call(dbi))
		return SQLITE_DEADCONN;

	int ret   = resp.get_i32();
	int ncols = resp.get_i32();
	ps->columns.resize(ncols);

	if (ret == SQLITE_ROW) {
		for (int i = 0; i < ncols; i++) {
			auto &c = ps->columns[i];
			c.clear();
			c.type = resp.get_i32();

			switch (c.type) {
			case SQLITE_INTEGER:
				c.i64 = resp.get_i64();
				break;
			case SQLITE_FLOAT:
				c.dbl = resp.get_double();
				break;
			case SQLITE_TEXT: {
				const char *t = resp.get_str();
				if (t)
					c.blob = t;
				c.text_converted = true;
				break;
			}
			case SQLITE_BLOB: {
				uint32_t blen = 0;
				const void *bd = resp.get_blob(blen);
				if (bd && blen > 0)
					c.blob.assign(static_cast<const char *>(bd), blen);
				break;
			}
			default:
				break;
			}
		}
	} else {
		for (auto &c : ps->columns)
			c.clear();
	}

	const char *err = resp.get_str();
	if (err != nullptr)
		dbi.errmsg = err;
	else
		dbi.errmsg.clear();
	return ret;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_reset(sqlite3_stmt *ps) try
{
	if (ps == nullptr)
		return SQLITE_MISUSE;
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_OK;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_RESET);
	req.put_u64(ps->remote_id);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_finalize(sqlite3_stmt *ps) try
{
	if (ps == nullptr)
		return SQLITE_OK;
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_OK;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_FINALIZE);
	req.put_u64(ps->remote_id);
	int ret = rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
	dbi.stmts.erase(ps->remote_id);
	return ret;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_bind_null(sqlite3_stmt *ps, int col) try
{
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DEADCONN;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_BIND_NULL);
	req.put_u64(ps->remote_id);
	req.put_i32(col);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_bind_int(sqlite3_stmt *ps, int col, int val)
{
	return sqlite3_bind_int64(ps, col, val);
}

GX_EXPORT int sqlite3_bind_int64(sqlite3_stmt *ps, int col,
    sqlite3_int64 val) try
{
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DEADCONN;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_BIND_INT64);
	req.put_u64(ps->remote_id);
	req.put_i32(col);
	req.put_i64(val);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_bind_text(sqlite3_stmt *ps, int col, const char *val,
    int n, void (*xDel)(void *)) try
{
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DEADCONN;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;

	if (val == nullptr) {
		req.clear();
		req.put_u8(OP_BIND_NULL);
		req.put_u64(ps->remote_id);
		req.put_i32(col);
		return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
	}

	uint32_t len = n >= 0 ? n : strlen(val);
	req.clear();
	req.put_u8(OP_BIND_TEXT);
	req.put_u64(ps->remote_id);
	req.put_i32(col);
	req.put_blob(val, len);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_bind_blob64(sqlite3_stmt *ps, int col, const void *val,
    sqlite3_uint64 n, void (*xDel)(void *)) try
{
	if (n >= 2047 * 1024 * 1024)
		return SQLITE_TOOBIG;
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DEADCONN;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_BIND_BLOB);
	req.put_u64(ps->remote_id);
	req.put_i32(col);
	req.put_blob(val, n);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}


GX_EXPORT int sqlite3_bind_blob(sqlite3_stmt *ps, int col, const void *val,
    int n, void (*xDel)(void *))
{
	return sqlite3_bind_blob64(ps, col, val, n, xDel);
}

GX_EXPORT int sqlite3_bind_double(sqlite3_stmt *ps, int col, double val) try
{
	auto db = ps->db.lock();
	if (db == nullptr)
		return SQLITE_DEADCONN;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_BIND_DOUBLE);
	req.put_u64(ps->remote_id);
	req.put_i32(col);
	req.put_double(val);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

/**
 * Column accessors (from cached step data, no IPC)
 *
 * Safe without mutex because sqlite3's API contract requires that column
 * accessors are only called between step() and the next step()/reset()/
 * finalize() on the same statement -- same thread, no concurrent access.
 */
GX_EXPORT sqlite3_int64 sqlite3_column_int64(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return 0;
	auto &c = ps->columns[col];
	switch (c.type) {
	case SQLITE_INTEGER:
		return c.i64;
	case SQLITE_FLOAT:
		return static_cast<int64_t>(c.dbl);
	case SQLITE_TEXT:
		return c.blob.empty() ? 0 : strtoll(c.blob.c_str(), nullptr, 10);
	default:
		return 0;
	}
}


GX_EXPORT int sqlite3_column_int(sqlite3_stmt *ps, int col)
{
	return sqlite3_column_int64(ps, col);
}

GX_EXPORT const unsigned char *sqlite3_column_text(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return nullptr;
	auto &c = ps->columns[col];
	if (c.type == SQLITE_NULL)
		return nullptr;

	// Lazily convert to text and cache; subsequent calls return the same
	// pointer, matching sqlite3's stability guarantee.
	if (!c.text_converted) {
		switch (c.type) {
		case SQLITE_INTEGER:
			c.blob = std::to_string(c.i64);
			break;
		case SQLITE_FLOAT: {
			char buf[64];
			snprintf(buf, sizeof(buf), "%.17g", c.dbl);
			c.blob = buf;
			break;
		}
		default:
			break;
		}
		c.text_converted = true;
	}
	return reinterpret_cast<const unsigned char *>(c.blob.c_str());
}

GX_EXPORT const void *sqlite3_column_blob(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return nullptr;
	auto &c = ps->columns[col];
	switch (c.type) {
	case SQLITE_TEXT: [[fallthrough]];
	case SQLITE_BLOB: return c.blob.data();
	default:          return nullptr;
	}
}

GX_EXPORT int sqlite3_column_bytes(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return 0;
	auto &c = ps->columns[col];
	switch (c.type) {
	case SQLITE_TEXT: [[fallthrough]];
	case SQLITE_BLOB: return static_cast<int>(c.blob.size());
	default:          return 0;
	}
}

GX_EXPORT int sqlite3_column_type(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return SQLITE_NULL;
	return ps->columns[col].type;
}

GX_EXPORT double sqlite3_column_double(sqlite3_stmt *ps, int col)
{
	if (col < 0 || static_cast<size_t>(col) >= ps->columns.size())
		return 0;
	auto &c = ps->columns[col];
	switch (c.type) {
	case SQLITE_FLOAT:   return c.dbl;
	case SQLITE_INTEGER: return c.i64;
	case SQLITE_TEXT:    return c.blob.empty() ? 0 : strtod(c.blob.c_str(), nullptr);
	default:             return 0;
	}
}

GX_EXPORT int sqlite3_column_count(sqlite3_stmt *ps)
{
	return ps != nullptr ? ps->columns.size() : 0;
}

GX_EXPORT int sqlite3_exec(sqlite3 *db, const char *sql,
    int (*cb)(void *, int, char **, char **), void *arg, char **errmsg) try
{
	if (errmsg != nullptr)
		*errmsg = nullptr;
	if (cb != nullptr)
		return SQLITE_MISUSE;
	if (db == nullptr || sql == nullptr)
		return SQLITE_MISUSE;

	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req  = dbi.rpc_req;
	auto &resp = dbi.rpc_resp;
	req.clear();
	req.put_u8(OP_EXEC);
	req.put_str(sql);
	if (!rpc_call(dbi))
		return SQLITE_DEADCONN;

	int ret = resp.get_i32();
	const char *msg = resp.get_str();
	if (msg != nullptr)
		dbi.errmsg = msg;
	else
		dbi.errmsg.clear();
	if (ret != SQLITE_OK && errmsg != nullptr && !dbi.errmsg.empty())
		*errmsg = strdup(dbi.errmsg.c_str());
	return ret;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT const char *sqlite3_errmsg(sqlite3 *db) try
{
	if (db == nullptr)
		return "not an error";
	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_ERRMSG);
	if (!rpc_call(dbi))
		return "IPC error";
	const char *msg = dbi.rpc_resp.get_str();
	if (msg != nullptr)
		dbi.errmsg = msg;
	else
		dbi.errmsg.clear();
	return dbi.errmsg.c_str();
} catch (const sw_pkt_error &) {
	return sqlite3_errstr(SQLITE_PKTERR);
} catch (const std::bad_alloc &) {
	return sqlite3_errstr(SQLITE_NOMEM);
}

static constexpr const char *const aMsg[] = {
	/* SQLITE_OK          */ "not an error",
	/* SQLITE_ERROR       */ "SQL logic error",
	/* SQLITE_INTERNAL    */ 0,
	/* SQLITE_PERM        */ "access permission denied",
	/* SQLITE_DEADCONN       */ "query aborted",
	/* SQLITE_BUSY        */ "database is locked",
	/* SQLITE_LOCKED      */ "database table is locked",
	/* SQLITE_NOMEM       */ "out of memory",
	/* SQLITE_READONLY    */ "attempt to write a readonly database",
	/* SQLITE_INTERRUPT   */ "interrupted",
	/* SQLITE_IOERR       */ "disk I/O error",
	/* SQLITE_CORRUPT     */ "database disk image is malformed",
	/* SQLITE_NOTFOUND    */ "unknown operation",
	/* SQLITE_FULL        */ "database or disk is full",
	/* SQLITE_CANTOPEN    */ "unable to open database file",
	/* SQLITE_PROTOCOL    */ "locking protocol",
	/* SQLITE_EMPTY       */ 0,
	/* SQLITE_SCHEMA      */ "database schema has changed",
	/* SQLITE_TOOBIG      */ "string or blob too big",
	/* SQLITE_CONSTRAINT  */ "constraint failed",
	/* SQLITE_MISMATCH    */ "datatype mismatch",
	/* SQLITE_MISUSE      */ "bad parameter or other API misuse",
	/* SQLITE_NOLFS       */ "large file support is disabled",
	/* SQLITE_AUTH        */ "authorization denied",
	/* SQLITE_FORMAT      */ 0,
	/* SQLITE_RANGE       */ "column index out of range",
	/* SQLITE_NOTADB      */ "file is not a database",
	/* SQLITE_NOTICE      */ "notification message",
	/* SQLITE_WARNING     */ "warning message",
};

GX_EXPORT const char *sqlite3_errstr(int errcode)
{
	if (errcode == SQLITE_DEADCONN)
		return "Connection to sqlite-worker subprocess died";
	if (errcode == SQLITE_PKTERR)
		return "Packet error while communicating with subprocess";
	if (errcode < 0 || static_cast<size_t>(errcode) >= std::size(aMsg))
		return "unknown error";
	return aMsg[errcode];
}

GX_EXPORT sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db) try
{
	if (db == nullptr)
		return 0;
	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_LAST_INSERT_ROWID);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i64() : 0;
} catch (const sw_pkt_error &) {
	return -EILSEQ;
} catch (const std::bad_alloc &) {
	return -ENOMEM;
}

GX_EXPORT int sqlite3_busy_timeout(sqlite3 *db, int ms) try
{
	if (db == nullptr)
		return SQLITE_MISUSE;
	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_BUSY_TIMEOUT);
	req.put_i32(ms);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_DEADCONN;
} catch (const sw_pkt_error &) {
	return SQLITE_PKTERR;
} catch (const std::bad_alloc &) {
	return SQLITE_NOMEM;
}

GX_EXPORT int sqlite3_txn_state(sqlite3 *db, const char *zSchema) try
{
	if (db == nullptr)
		return SQLITE_TXN_NONE;
	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_TXN_STATE);
	req.put_str(zSchema);
	return rpc_call(dbi) ? dbi.rpc_resp.get_i32() : SQLITE_TXN_NONE;
} catch (const sw_pkt_error &) {
	return -EILSEQ;
} catch (const std::bad_alloc &) {
	return -ENOMEM;
}

GX_EXPORT const char *sqlite3_db_filename(sqlite3 *db, const char *zDbName) try
{
	if (db == nullptr)
		return nullptr;
	auto &dbi = *db->m_impl;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_DB_FILENAME);
	req.put_str(zDbName);
	if (!rpc_call(dbi))
		return nullptr;
	const char *fn = dbi.rpc_resp.get_str();
	if (fn != nullptr)
		dbi.db_filename = fn;
	return dbi.db_filename.c_str();
} catch (const sw_pkt_error &) {
	return sqlite3_errstr(SQLITE_PKTERR);
} catch (const std::bad_alloc &) {
	return sqlite3_errstr(SQLITE_NOMEM);
}

GX_EXPORT sqlite3 *sqlite3_db_handle(sqlite3_stmt *ps)
{
	auto db = ps->db.lock();
	if (db == nullptr)
		return nullptr;
	return ps->rdb;
}

GX_EXPORT char *sqlite3_expanded_sql(sqlite3_stmt *ps) try
{
	if (ps == nullptr)
		return nullptr;
	auto db = ps->db.lock();
	if (db == nullptr)
		return nullptr;
	auto &dbi = *db;
	std::lock_guard lk(dbi.mtx);
	auto &req = dbi.rpc_req;
	req.clear();
	req.put_u8(OP_EXPANDED_SQL);
	req.put_u64(ps->remote_id);
	if (!rpc_call(dbi))
		return nullptr;
	const char *sql = dbi.rpc_resp.get_str();
	return sql != nullptr ? strdup(sql) : nullptr;
} catch (const sw_pkt_error &) {
	return strdup(sqlite3_errstr(SQLITE_PKTERR));
} catch (const std::bad_alloc &) {
	return NULL;
}

GX_EXPORT const char *sqlite3_sql(sqlite3_stmt *ps)
{
	if (ps == nullptr)
		return nullptr;
	return ps->sql_text.c_str();
}

GX_EXPORT void sqlite3_free(void *ptr) { free(ptr); }
GX_EXPORT int sqlite3_config(int, ...) { return SQLITE_OK; }
GX_EXPORT int sqlite3_initialize() { return SQLITE_OK; }
GX_EXPORT int sqlite3_shutdown() { return SQLITE_OK; }
