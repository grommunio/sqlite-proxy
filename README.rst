Address-Space-Isolated sqlite
=============================

This is a LD_PRELOAD-able (drop-in) library that replaces the existing sqlite3
library with a proxy implementation that forwards all operations to a
subordinate process with separate address space. All sqlite3 data structures
live in the subprocess; the parent only holds proxy handles.

Limitations
-----------

* Only a subset of sqlite3 APIs is implemented.
* `sqlite3_exec` callbacks are not supported.
* `pzTail` output from `sqlite3_prepare_v2` is not supported.
* `sqlite3_column_count` returns 0 before the first `sqlite3_step` call.
* Worker termination effectively makes the `sqlite3` handle unusable.

The thread safety model matches sqlite3 MULTITHREAD mode. Different connections
may be used concurrently from different threads. A single connection (and its
statements) must not be used concurrently from multiple threads.

Worker termination
------------------

When the worker process for a given `sqlite3` handle exits unexpectedly, the
proxy implementation (in the superior process) will not restart that worker.
Instead, the proxy returns an error code for all subsequent function calls
(e.g. sqlite3_step) made with the handle. This is because things like cursors,
transactions and attachments (made with ``ATTACH DATABASE ...``) are lost at
that point. The superior process must deal with this situation, and completely
shut and reopen the database with e.g. sqlite3_close & sqlite3_open_v2 again.

Usage
-----

.. code-block:: sh

	LD_LIBRARY_PATH=/usr/lib/sqlite-proxy SQLITE_WORKER=/usr/libexec/sqlite-proxy/sqlite-worker your-program

or when run from just-built sources:

.. code-block:: sh

	LD_LIBRARY_PATH=lib/.libs SQLITE_WORKER=./sqlite-worker ./testproxy
