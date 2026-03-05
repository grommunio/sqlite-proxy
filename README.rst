Address-Space-Isolated sqlite
=============================

This is a LD_PRELOAD-able (drop-in) library that replaces the existing sqlite3
library with a proxy implementation that forwards all operations to a
subordinate process with separate address space. All sqlite3 data structures
live in the subprocess; the parent only holds proxy handles.

Limitations:

* Only a subset of sqlite3 APIs is implemented.
* `sqlite3_exec` callbacks are not supported.
* `pzTail` output from `sqlite3_prepare_v2` is not supported.
* `sqlite3_column_count` returns 0 before the first `sqlite3_step` call.

The thread safety model matches sqlite3 MULTITHREAD mode. Different connections
may be used concurrently from different threads. A single connection (and its
statements) must not be used concurrently from multiple threads.

Usage:

.. code-block:: sh

	LD_LIBRARY_PATH=/usr/lib/sqlite-proxy SQLITE_WORKER=/usr/libexec/sqlite-proxy/sqlite-worker your-program

or when run from just-built sources:

.. code-block:: sh

	LD_LIBRARY_PATH=lib/.libs SQLITE_WORKER=./sqlite-worker ./testproxy
