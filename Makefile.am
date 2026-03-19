# -*- Makefile -*-

ACLOCAL_AMFLAGS = -I build-aux
AM_CPPFLAGS = ${my_CPPFLAGS} ${libHX_CFLAGS} ${sqlite_CFLAGS}
AM_CFLAGS   = ${my_CFLAGS}
AM_CXXFLAGS = ${my_CXXFLAGS}
AM_LDFLAGS  = ${my_LDFLAGS}

#
# libsqlite3 and sqlite-worker should not be in the same directory:
# When running $PWD/sqlite-worker (the libtool script), $PWD/.libs
# is added to LD_LIBRARY_PATH again, at which point it would pick
# up the proxy libsqlite3.so.
#

pkglibexec_PROGRAMS = sqlite-worker
xlibdir = ${libdir}/${PACKAGE_NAME}
xlib_LTLIBRARIES = lib/libsqlite3.la
noinst_PROGRAMS = testproxy

sqlite_worker_SOURCES = worker.cpp
sqlite_worker_LDADD = ${sqlite_LIBS}
lib_libsqlite3_la_SOURCES = proxy.cpp
lib_libsqlite3_la_LDFLAGS = -Wl,--version-script=${srcdir}/proxy.sym
EXTRA_lib_libsqlite3_la_DEPENDENCIES = proxy.sym
testproxy_SOURCES = testproxy.cpp
testproxy_LDADD = ${sqlite_LIBS}
