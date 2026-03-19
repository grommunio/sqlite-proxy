// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 grommunio GmbH
#include <cassert>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <sqlite3.h>

#define CHECK(cond, ...) do { \
	if (!(cond)) { \
		fprintf(stderr, "FAIL at %s:%d: ", __FILE__, __LINE__); \
		fprintf(stderr, __VA_ARGS__); \
		fprintf(stderr, "\n"); \
		exit(1); \
	} \
} while (0)

#define CHECK_OK(rc) CHECK((rc) == SQLITE_OK, "expected SQLITE_OK, got %d (%s)", (rc), sqlite3_errstr(rc))

static void test_open_close()
{
	printf("test_open_close... ");
	sqlite3 *db = nullptr;
	int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
	CHECK_OK(rc);
	CHECK(db != nullptr, "db is null");
	rc = sqlite3_close_v2(db);
	CHECK_OK(rc);
	printf("OK\n");
}

static void test_exec()
{
	printf("test_exec... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(1, 'hello')", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(2, 'world')", nullptr, nullptr, nullptr));

	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_prepare_step()
{
	printf("test_prepare_step... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT, val REAL)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(1, 'alpha', 1.5)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(2, 'beta', 2.7)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(3, NULL, 3.14)", nullptr, nullptr, nullptr));

	sqlite3_stmt *stmt = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT id, name, val FROM t ORDER BY id", -1, &stmt, nullptr));
	CHECK(stmt != nullptr, "stmt is null");

	// Row 1
	int rc = sqlite3_step(stmt);
	CHECK(rc == SQLITE_ROW, "expected ROW, got %d", rc);
	CHECK(sqlite3_column_int64(stmt, 0) == 1, "id mismatch");
	CHECK(strcmp(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1)), "alpha") == 0, "name mismatch");
	CHECK(sqlite3_column_type(stmt, 1) == SQLITE_TEXT, "type mismatch");
	double v = sqlite3_column_double(stmt, 2);
	CHECK(v > 1.4 && v < 1.6, "val mismatch: %f", v);

	// Row 2
	rc = sqlite3_step(stmt);
	CHECK(rc == SQLITE_ROW, "expected ROW, got %d", rc);
	CHECK(sqlite3_column_int64(stmt, 0) == 2, "id mismatch");

	// Row 3 - NULL name
	rc = sqlite3_step(stmt);
	CHECK(rc == SQLITE_ROW, "expected ROW, got %d", rc);
	CHECK(sqlite3_column_int64(stmt, 0) == 3, "id mismatch");
	CHECK(sqlite3_column_type(stmt, 1) == SQLITE_NULL, "expected NULL type");

	// No more rows
	rc = sqlite3_step(stmt);
	CHECK(rc == SQLITE_DONE, "expected DONE, got %d", rc);

	sqlite3_finalize(stmt);
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_bind()
{
	printf("test_bind... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER, name TEXT, data BLOB)", nullptr, nullptr, nullptr));

	sqlite3_stmt *ins = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?, ?, ?)", -1, &ins, nullptr));

	// Insert with int64, text, blob
	CHECK_OK(sqlite3_bind_int64(ins, 1, 42));
	CHECK_OK(sqlite3_bind_text(ins, 2, "test_text", -1, SQLITE_STATIC));
	const uint8_t blob[] = {0xDE, 0xAD, 0xBE, 0xEF};
	CHECK_OK(sqlite3_bind_blob(ins, 3, blob, sizeof(blob), SQLITE_STATIC));
	CHECK(sqlite3_step(ins) == SQLITE_DONE, "insert failed");

	// Insert with NULL
	CHECK_OK(sqlite3_reset(ins));
	CHECK_OK(sqlite3_bind_int64(ins, 1, 99));
	CHECK_OK(sqlite3_bind_null(ins, 2));
	CHECK_OK(sqlite3_bind_null(ins, 3));
	CHECK(sqlite3_step(ins) == SQLITE_DONE, "insert failed");

	sqlite3_finalize(ins);

	// Read back
	sqlite3_stmt *sel = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT id, name, data FROM t WHERE id=42", -1, &sel, nullptr));
	CHECK(sqlite3_step(sel) == SQLITE_ROW, "no row");
	CHECK(sqlite3_column_int64(sel, 0) == 42, "id wrong");
	CHECK(strcmp(reinterpret_cast<const char *>(sqlite3_column_text(sel, 1)), "test_text") == 0, "text wrong");
	CHECK(sqlite3_column_bytes(sel, 2) == 4, "blob size wrong");
	const auto *bdata = static_cast<const uint8_t *>(sqlite3_column_blob(sel, 2));
	CHECK(bdata[0] == 0xDE && bdata[3] == 0xEF, "blob data wrong");

	sqlite3_finalize(sel);
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_last_insert_rowid()
{
	printf("test_last_insert_rowid... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t(val) VALUES('a')", nullptr, nullptr, nullptr));
	CHECK(sqlite3_last_insert_rowid(db) == 1, "rowid mismatch");
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t(val) VALUES('b')", nullptr, nullptr, nullptr));
	CHECK(sqlite3_last_insert_rowid(db) == 2, "rowid mismatch");
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_txn_state()
{
	printf("test_txn_state... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER)", nullptr, nullptr, nullptr));

	CHECK(sqlite3_txn_state(db, "main") == SQLITE_TXN_NONE, "expected TXN_NONE");

	CHECK_OK(sqlite3_exec(db, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr));
	CHECK(sqlite3_txn_state(db, "main") == SQLITE_TXN_WRITE, "expected TXN_WRITE");

	CHECK_OK(sqlite3_exec(db, "COMMIT", nullptr, nullptr, nullptr));
	CHECK(sqlite3_txn_state(db, "main") == SQLITE_TXN_NONE, "expected TXN_NONE after commit");

	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_busy_timeout()
{
	printf("test_busy_timeout... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_busy_timeout(db, 5000));
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_errmsg()
{
	printf("test_errmsg... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	// Provoke an error
	char *err = nullptr;
	int rc = sqlite3_exec(db, "SELECT * FROM nonexistent_table", nullptr, nullptr, &err);
	CHECK(rc != SQLITE_OK, "expected error");
	if (err) {
		CHECK(strstr(err, "nonexistent_table") != nullptr || strstr(err, "no such table") != nullptr,
		      "error message doesn't mention table: %s", err);
		sqlite3_free(err);
	}

	const char *msg = sqlite3_errmsg(db);
	CHECK(msg != nullptr, "errmsg null");

	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_errstr()
{
	printf("test_errstr... ");
	const char *s = sqlite3_errstr(SQLITE_BUSY);
	CHECK(s != nullptr, "errstr null");
	CHECK(strstr(s, "locked") != nullptr || strstr(s, "busy") != nullptr,
	      "unexpected errstr: %s", s);
	printf("OK\n");
}

static void test_db_filename()
{
	printf("test_db_filename... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	const char *fn = sqlite3_db_filename(db, nullptr);
	// :memory: databases have empty string filename
	CHECK(fn != nullptr, "filename null");
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_db_handle()
{
	printf("test_db_handle... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER)", nullptr, nullptr, nullptr));

	sqlite3_stmt *stmt = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT 1", -1, &stmt, nullptr));
	sqlite3 *got_db = sqlite3_db_handle(stmt);
	CHECK(got_db == db, "db_handle mismatch");

	sqlite3_finalize(stmt);
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_sql_and_expanded()
{
	printf("test_sql_and_expanded... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	sqlite3_stmt *stmt = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT ? + ?", -1, &stmt, nullptr));
	CHECK_OK(sqlite3_bind_int64(stmt, 1, 10));
	CHECK_OK(sqlite3_bind_int64(stmt, 2, 20));

	const char *sql = sqlite3_sql(stmt);
	CHECK(sql != nullptr && strcmp(sql, "SELECT ? + ?") == 0, "sql mismatch: %s", sql);

	char *exp = sqlite3_expanded_sql(stmt);
	CHECK(exp != nullptr, "expanded_sql null");
	CHECK(strstr(exp, "10") != nullptr && strstr(exp, "20") != nullptr,
	      "expanded_sql doesn't contain bound values: %s", exp);
	sqlite3_free(exp);

	sqlite3_finalize(stmt);
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_reset_rebind()
{
	printf("test_reset_rebind... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(1)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(2)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(3)", nullptr, nullptr, nullptr));

	sqlite3_stmt *stmt = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT id FROM t WHERE id=?", -1, &stmt, nullptr));

	// First query
	CHECK_OK(sqlite3_bind_int64(stmt, 1, 2));
	CHECK(sqlite3_step(stmt) == SQLITE_ROW, "no row");
	CHECK(sqlite3_column_int64(stmt, 0) == 2, "wrong id");
	CHECK(sqlite3_step(stmt) == SQLITE_DONE, "expected done");

	// Reset and rebind
	CHECK_OK(sqlite3_reset(stmt));
	CHECK_OK(sqlite3_bind_int64(stmt, 1, 3));
	CHECK(sqlite3_step(stmt) == SQLITE_ROW, "no row");
	CHECK(sqlite3_column_int64(stmt, 0) == 3, "wrong id");

	sqlite3_finalize(stmt);
	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_bind_double()
{
	printf("test_bind_double... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(val REAL)", nullptr, nullptr, nullptr));

	sqlite3_stmt *ins = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "INSERT INTO t VALUES(?)", -1, &ins, nullptr));
	CHECK_OK(sqlite3_bind_double(ins, 1, 3.14159));
	CHECK(sqlite3_step(ins) == SQLITE_DONE, "insert failed");
	sqlite3_finalize(ins);

	sqlite3_stmt *sel = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db, "SELECT val FROM t", -1, &sel, nullptr));
	CHECK(sqlite3_step(sel) == SQLITE_ROW, "no row");
	double v = sqlite3_column_double(sel, 0);
	CHECK(v > 3.14 && v < 3.15, "double mismatch: %f", v);
	sqlite3_finalize(sel);

	sqlite3_close_v2(db);
	printf("OK\n");
}

static void test_multiple_connections()
{
	printf("test_multiple_connections... ");
	sqlite3 *db1 = nullptr, *db2 = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db1, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));
	CHECK_OK(sqlite3_open_v2(":memory:", &db2, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	CHECK_OK(sqlite3_exec(db1, "CREATE TABLE t(id INTEGER)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db2, "CREATE TABLE t(id INTEGER)", nullptr, nullptr, nullptr));

	CHECK_OK(sqlite3_exec(db1, "INSERT INTO t VALUES(111)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db2, "INSERT INTO t VALUES(222)", nullptr, nullptr, nullptr));

	sqlite3_stmt *s1 = nullptr, *s2 = nullptr;
	CHECK_OK(sqlite3_prepare_v2(db1, "SELECT id FROM t", -1, &s1, nullptr));
	CHECK_OK(sqlite3_prepare_v2(db2, "SELECT id FROM t", -1, &s2, nullptr));

	CHECK(sqlite3_step(s1) == SQLITE_ROW, "no row");
	CHECK(sqlite3_column_int64(s1, 0) == 111, "db1 data wrong");

	CHECK(sqlite3_step(s2) == SQLITE_ROW, "no row");
	CHECK(sqlite3_column_int64(s2, 0) == 222, "db2 data wrong");

	sqlite3_finalize(s1);
	sqlite3_finalize(s2);
	sqlite3_close_v2(db1);
	sqlite3_close_v2(db2);
	printf("OK\n");
}

static void test_config_init_shutdown()
{
	printf("test_config_init_shutdown... ");
	// sqlite3_config can only be called before initialize or after shutdown.
	// Since we already used sqlite3 above, just test init/shutdown.
	CHECK_OK(sqlite3_initialize());
	CHECK_OK(sqlite3_shutdown());
	// After shutdown, config should work (but may not on all builds,
	// so we just test that it doesn't crash)
	sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
	sqlite3_initialize();
	printf("OK\n");
}

static void test_null_finalize()
{
	printf("test_null_finalize... ");
	// sqlite3_finalize(NULL) should be a no-op
	CHECK_OK(sqlite3_finalize(nullptr));
	// sqlite3_close_v2(NULL) should be a no-op
	CHECK_OK(sqlite3_close_v2(nullptr));
	printf("OK\n");
}

static void test_unexpected_abort()
{
	printf("test_unexpected_abort... ");
	sqlite3 *db = nullptr;
	CHECK_OK(sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr));

	CHECK_OK(sqlite3_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", nullptr, nullptr, nullptr));
	CHECK_OK(sqlite3_exec(db, "INSERT INTO t VALUES(1, 'hello')", nullptr, nullptr, nullptr));

	void *fh = dlopen("libsqlite3.so.0", RTLD_NOW);
	CHECK(fh != nullptr, "dlopen=NULL");
	int (*t_close)(sqlite3 *);
	t_close = reinterpret_cast<decltype(t_close)>(dlsym(fh, "sqlite_proxy_close_nodel"));
	CHECK(t_close != nullptr, "t_close=NULL");
	t_close(db);
	auto ret = sqlite3_exec(db, "INSERT INTO t VALUES(2, 'world')", nullptr, nullptr, nullptr);
	CHECK(ret == 96, "sqlite3_exec==96");
	CHECK_OK(sqlite3_close_v2(db));
	printf("OK\n");
}

int main()
{
	signal(SIGPIPE, SIG_IGN);
	setvbuf(stdout, nullptr, _IONBF, 0);
	printf("=== sqlite3 preload test suite ===\n");

	test_open_close();
	test_exec();
	test_prepare_step();
	test_bind();
	test_last_insert_rowid();
	test_txn_state();
	test_busy_timeout();
	test_errmsg();
	test_errstr();
	test_db_filename();
	test_db_handle();
	test_sql_and_expanded();
	test_reset_rebind();
	test_bind_double();
	test_multiple_connections();
	test_config_init_shutdown();
	test_null_finalize();
	test_unexpected_abort();

	printf("\n=== All tests passed! ===\n");
	return 0;
}
