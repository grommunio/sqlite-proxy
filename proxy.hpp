#pragma once
#include <algorithm>
#include <cstdint>
#include <string>
#include <sqlite3.h>
#include <libHX/endian.h>

enum sw_op : uint8_t {
	OP_OPEN_V2 = 1,
	OP_CLOSE,
	OP_PREPARE_V2,
	OP_STEP,
	OP_RESET,
	OP_FINALIZE,
	OP_BIND_NULL,
	OP_BIND_INT64,
	OP_BIND_TEXT,
	OP_BIND_BLOB,
	OP_BIND_DOUBLE,
	OP_EXEC,
	OP_ERRMSG,
	OP_LAST_INSERT_ROWID,
	OP_BUSY_TIMEOUT,
	OP_TXN_STATE,
	OP_DB_FILENAME,
	OP_EXPANDED_SQL,
	OP_SQL,
};

struct sw_pkt_error : public std::runtime_error {
	private:
	using base_t = std::runtime_error;
	public:
	using base_t::base_t;
};

static bool wire_write_all(int fd, const void *buf, size_t len);
static bool wire_read_all(int fd, void *buf, size_t len);

class msg_buf {
	public:
	void put_u8(uint8_t v) { m_data.push_back(v); }
	void put_u32(uint32_t v) {
		char buf[4];
		cpu_to_le32p(buf, v);
		m_data.append(buf, std::size(buf));
	}
	void put_u64(uint64_t v) {
		char buf[8];
		cpu_to_le64p(buf, v);
		m_data.append(buf, std::size(buf));
	}
	void put_i32(int32_t v) { return put_u32(v); } /* same on wire */
	void put_i64(int64_t v) { return put_u64(v); } /* same on wire */
	void put_double(double v) {
		uint64_t w;
		memcpy(&w, &v, sizeof(v));
		return put_u64(w);
	}

	void put_str(const char *s)
	{
		if (s == nullptr)
			return put_blob(nullptr, 0);
		/*
		 * Always put a \0 on the wire, so that the receiver
		 * has a C-compatible string (not just a string_view).
		 */
		return put_blob(s, static_cast<uint32_t>(strlen(s)) + 1);
	}

	void put_blob(const void *d, uint32_t len)
	{
		put_u32(d != nullptr);
		if (d == nullptr)
			return;
		put_u32(len);
		if (len == 0)
			return;
		auto p = static_cast<const char *>(d);
		m_data.append(p, len);
	}

	uint8_t get_u8()
	{
		if (m_pos >= m_data.size())
			throw sw_pkt_error("get_u8: data larger than bytes available");
		return m_data[m_pos++];
	}
	uint32_t get_u32() {
		if (m_pos + 4 > m_data.size())
			throw sw_pkt_error("get_u32: data larger than bytes available");
		auto v = le32p_to_cpu(&m_data[m_pos]);
		m_pos += 4;
		return v;
	}
	uint32_t get_u64() {
		if (m_pos + 8 > m_data.size())
			throw sw_pkt_error("get_u64: data larger than bytes available");
		auto v = le64p_to_cpu(&m_data[m_pos]);
		m_pos += 8;
		return v;
	}
	int32_t get_i32() { return get_u32(); }
	int64_t get_i64() { return get_u64(); }

	double get_double()
	{
		if (m_pos + 8 > m_data.size())
			throw sw_pkt_error("get_double: data larger than bytes available");
		auto w = le64p_to_cpu(&m_data[m_pos]);
		double v;
		memcpy(&v, &w, sizeof(v));
		m_pos += 8;
		return v;
	}

	const char *get_str()
	{
		uint32_t len = 0;
		auto p = static_cast<const char *>(get_blob(len));
		if (p == nullptr || len == 0)
			return nullptr;
		if (p[len-1] != '\0')
			throw sw_pkt_error("get_str: packet format error: string not terminated");
		/* C string (i.e. \0-terminated) guaranteed now */
		return p;
	}

	const void *get_blob(uint32_t &out_len)
	{
		auto have_blob = get_u32();
		if (!have_blob) {
			out_len = 0;
			return nullptr;
		}
		out_len = get_u32();
		if (m_pos + out_len > m_data.size())
			throw sw_pkt_error("get_blob: data larger than bytes available");
		const void *p = &m_data[m_pos];
		m_pos += out_len;
		return p;
	}

	void clear()
	{
		m_data.clear();
		m_pos = 0;
	}

	bool send(int fd) const
	{
		uint32_t total = std::min(static_cast<size_t>(UINT32_MAX), m_data.size());
		char lbuf[4];
		cpu_to_le32p(lbuf, total);
		if (!wire_write_all(fd, lbuf, std::size(lbuf)))
			return false;
		if (total == 0)
			return true;
		return wire_write_all(fd, m_data.data(), total);
	}

	bool recv(int fd)
	{
		char lbuf[4];
		if (!wire_read_all(fd, lbuf, std::size(lbuf)))
			return false;
		auto total = le32p_to_cpu(lbuf);
		if (total >= 2047 * 1024 * 1024)
			return false;
		m_data.resize(total);
		m_pos = 0;
		if (total == 0)
			return true;
		return wire_read_all(fd, m_data.data(), total);
	}

	protected:
	std::string m_data;
	size_t m_pos = 0;
};

class sqlfree {
	public:
	void operator()(sqlite3 *x) const { sqlite3_close_v2(x); }
	void operator()(sqlite3_stmt *x) const { sqlite3_finalize(x); }
	void operator()(char *x) const { sqlite3_free(x); }
};
