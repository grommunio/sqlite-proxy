// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 grommunio GmbH
// This file is part of sqlite-proxy
/*
 * Worker process for libsqlite3_preload.so. Receives sqlite3 commands over
 * stdin and executes them against real sqlite3, outputting the result over
 * stdout. This program must be exec()-uted with LD_PRELOAD not containing the
 * wrapper library and LD_LIBRARY_PATH not pointing to the wrapper, so that the
 * worker does not recursively try to call itself.
 */
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sqlite3.h>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <libHX/endian.h>
#include <libHX/scope.hpp>
#include <libHX/tie.hpp>
#include "proxy.hpp"

static inline const char *znul(const char *s)
{
	return s != nullptr ? s : "";
}

/* Not replacable by HXio_fullwrite (that one decidedly does not continue on errors) */
static bool wire_write_all(int fd, const void *buf, size_t len)
{
	auto p = static_cast<const uint8_t *>(buf);
	while (len > 0) {
		ssize_t n = write(fd, p, len);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return false; /* e.g. EPIPE */
		}
		p += n;
		len -= static_cast<size_t>(n);
	}
	return true;
}

/* Not replacable by HXio_fullread. */
static bool wire_read_all(int fd, void *buf, size_t len)
{
	auto p = static_cast<uint8_t *>(buf);
	while (len > 0) {
		ssize_t n = read(fd, p, len);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return false; /* e.g. EPIPE */
		} else if (n == 0) {
			return false; /* EOF */
		}
		p += n;
		len -= static_cast<size_t>(n);
	}
	return true;
}

static void send_columns(sqlite3_stmt *stmt, msg_buf &resp)
{
	auto ncols = sqlite3_column_count(stmt);
	resp.put_i32(ncols);
	for (decltype(ncols) i = 0; i < ncols; i++) {
		auto type = sqlite3_column_type(stmt, i);
		resp.put_i32(type);
		switch (type) {
		case SQLITE_INTEGER:
			resp.put_i64(sqlite3_column_int64(stmt, i));
			break;
		case SQLITE_FLOAT:
			resp.put_double(sqlite3_column_double(stmt, i));
			break;
		case SQLITE_TEXT:
			resp.put_str(reinterpret_cast<const char *>(sqlite3_column_text(stmt, i)));
			break;
		case SQLITE_BLOB: {
			int blen = sqlite3_column_bytes(stmt, i);
			if (blen < 0)
				throw std::runtime_error("sqlite3_column_bytes yielded <0, that is unexpected");
			resp.put_blob(sqlite3_column_blob(stmt, i), blen);
			break;
		}
		default:
			break;
		}
	}
}

int main() try
{
	signal(SIGPIPE, SIG_IGN);
	msg_buf req;
	std::unique_ptr<sqlite3, sqlfree> the_db;
	std::unordered_map<uint64_t, std::unique_ptr<sqlite3_stmt, sqlfree>> prepped;

	while (req.recv(STDIN_FILENO)) {
		msg_buf resp;
		auto opcode = req.get_u8();

		switch (opcode) {
		case OP_OPEN_V2: {
			auto filename = req.get_str();
			auto flags = req.get_i32();
			auto vfs = req.get_str();
			int ret = sqlite3_open_v2(znul(filename), &~HX::unique_tie(the_db), flags, vfs);
			resp.put_i32(ret);
			if (ret == SQLITE_OK && the_db)
				resp.put_str(sqlite3_db_filename(the_db.get(), nullptr));
			else
				resp.put_str(nullptr);
			break;
		}
		case OP_CLOSE: {
			prepped.clear();
			the_db.reset();
			resp.put_i32(SQLITE_OK);
			resp.send(STDOUT_FILENO);
			return 0;
		}
		case OP_PREPARE_V2: {
			auto sql     = req.get_str();
			auto nbyte   = req.get_i32();
			auto stmt_id = req.get_u64();
			if (!the_db) {
				resp.put_i32(SQLITE_MISUSE);
				resp.put_u8(0);
				resp.put_str("no database open");
				break;
			}
			std::unique_ptr<sqlite3_stmt, sqlfree> stmt;
			auto ret = sqlite3_prepare_v2(the_db.get(), znul(sql),
			           nbyte, &HX::unique_tie(stmt), nullptr);
			resp.put_i32(ret);
			if (ret == SQLITE_OK && stmt) {
				prepped[stmt_id] = std::move(stmt);
				resp.put_u8(1);
				resp.put_str(sqlite3_sql(prepped[stmt_id].get()));
			} else {
				resp.put_u8(0);
				resp.put_str(sqlite3_errmsg(the_db.get()));
			}
			break;
		}
		case OP_STEP: {
			auto sid = req.get_u64();
			auto it  = prepped.find(sid);
			if (it == prepped.end()) {
				resp.put_i32(SQLITE_MISUSE);
				resp.put_i32(0);
				resp.put_str(nullptr);
				break;
			}
			auto ret = sqlite3_step(it->second.get());
			resp.put_i32(ret);
			if (ret == SQLITE_ROW)
				send_columns(it->second.get(), resp);
			else
				resp.put_i32(0);
			if (ret != SQLITE_OK && ret != SQLITE_ROW && ret != SQLITE_DONE)
				resp.put_str(sqlite3_errmsg(the_db.get()));
			else
				resp.put_str(nullptr);
			break;
		}
		case OP_RESET: {
			auto sid = req.get_u64();
			auto it  = prepped.find(sid);
			resp.put_i32(it != prepped.end() ? sqlite3_reset(it->second.get()) : SQLITE_MISUSE);
			break;
		}
		case OP_FINALIZE: {
			auto sid = req.get_u64();
			auto it  = prepped.find(sid);
			int ret  = SQLITE_OK;
			if (it != prepped.end()) {
				ret = sqlite3_finalize(it->second.release());
				prepped.erase(it);
			}
			resp.put_i32(ret);
			break;
		}
		case OP_BIND_NULL: {
			auto sid = req.get_u64();
			auto col = req.get_i32();
			auto it  = prepped.find(sid);
			resp.put_i32(it != prepped.end() ? sqlite3_bind_null(it->second.get(), col) : SQLITE_MISUSE);
			break;
		}
		case OP_BIND_INT64: {
			auto sid = req.get_u64();
			auto col = req.get_i32();
			auto val = req.get_i64();
			auto it  = prepped.find(sid);
			resp.put_i32(it != prepped.end() ? sqlite3_bind_int64(it->second.get(), col, val) : SQLITE_MISUSE);
			break;
		}
		case OP_BIND_TEXT: {
			auto sid = req.get_u64();
			auto col = req.get_i32();
			uint32_t tlen = 0;
			const void *td = req.get_blob(tlen);
			auto it = prepped.find(sid);
			if (it != prepped.end())
				resp.put_i32(sqlite3_bind_text(it->second.get(), col,
					td != nullptr ? static_cast<const char *>(td) : nullptr,
					tlen, SQLITE_TRANSIENT));
			else
				resp.put_i32(SQLITE_MISUSE);
			break;
		}
		case OP_BIND_BLOB: {
			auto sid = req.get_u64();
			auto col = req.get_i32();
			uint32_t blen = 0;
			auto bd = req.get_blob(blen);
			auto it = prepped.find(sid);
			if (it != prepped.end())
				resp.put_i32(sqlite3_bind_blob(it->second.get(), col, bd,
					static_cast<int>(blen), SQLITE_TRANSIENT));
			else
				resp.put_i32(SQLITE_MISUSE);
			break;
		}
		case OP_BIND_DOUBLE: {
			auto sid = req.get_u64();
			auto col = req.get_i32();
			auto val = req.get_double();
			auto it  = prepped.find(sid);
			resp.put_i32(it != prepped.end() ? sqlite3_bind_double(it->second.get(), col, val) : SQLITE_MISUSE);
			break;
		}
		case OP_EXEC: {
			auto sql = req.get_str();
			if (!the_db) {
				resp.put_i32(SQLITE_MISUSE);
				resp.put_str("no database open");
				break;
			}
			std::unique_ptr<char[], sqlfree> estr;
			auto ret = sqlite3_exec(the_db.get(), znul(sql),
			           nullptr, nullptr, &~HX::unique_tie(estr));
			resp.put_i32(ret);
			resp.put_str(estr.get());
			break;
		}
		case OP_ERRMSG:
			resp.put_str(the_db ? sqlite3_errmsg(the_db.get()) : "no database open");
			break;
		case OP_LAST_INSERT_ROWID:
			resp.put_i64(the_db ? sqlite3_last_insert_rowid(the_db.get()) : 0);
			break;
		case OP_BUSY_TIMEOUT: {
			int ms = req.get_i32();
			resp.put_i32(the_db ? sqlite3_busy_timeout(the_db.get(), ms) : SQLITE_MISUSE);
			break;
		}
		case OP_TXN_STATE: {
			const char *schema = req.get_str();
			resp.put_i32(the_db ? sqlite3_txn_state(the_db.get(), schema) : SQLITE_TXN_NONE);
			break;
		}
		case OP_DB_FILENAME: {
			auto schema = req.get_str();
			resp.put_str(the_db ? sqlite3_db_filename(the_db.get(), schema) : nullptr);
			break;
		}
		case OP_EXPANDED_SQL: {
			uint64_t sid = req.get_u64();
			auto it = prepped.find(sid);
			if (it != prepped.end()) {
				std::unique_ptr<char[], sqlfree> exp(sqlite3_expanded_sql(it->second.get()));
				resp.put_str(exp.get());
			} else {
				resp.put_str(nullptr);
			}
			break;
		}
		case OP_SQL: {
			auto sid = req.get_u64();
			auto it  = prepped.find(sid);
			resp.put_str(it != prepped.end() ? sqlite3_sql(it->second.get()) : nullptr);
			break;
		}
		default:
			resp.put_i32(SQLITE_MISUSE);
			break;
		}

		if (!resp.send(STDOUT_FILENO))
			break;
	}
	return EXIT_SUCCESS;
} catch (const std::runtime_error &e) {
	fprintf(stderr, "%s\n", e.what());
	return EXIT_FAILURE;
}
