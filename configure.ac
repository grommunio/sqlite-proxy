AC_INIT([sqlite-proxy], [1.0])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([build-aux])
AC_PREFIX_DEFAULT([/usr])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([1.10 -Wall -Wno-portability foreign subdir-objects tar-pax])
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
LT_INIT([disable-static])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_PROG_CXX
AM_CONDITIONAL([WITH_GNU_LD], [test "$with_gnu_ld" = yes])

saved_CPPFLAGS="$CPPFLAGS"
saved_CFLAGS="$CFLAGS"
saved_CXXFLAGS="$CXXFLAGS"
saved_LDFLAGS="$LDFLAGS"
saved_LIBS="$LIBS"
AC_LANG_PUSH([C++])

my_CPPFLAGS="-D_FILE_OFFSET_BITS=64 -D_TIME_BITS=64"
my_CFLAGS="-Wall -Wmissing-declarations -Wwrite-strings"
my_CXXFLAGS="-Wall -Wmissing-declarations"
my_LDFLAGS=""
cxxmode="error"
AC_MSG_CHECKING([for C++ standard availability])
echo ""
for i in "" "c++26" "c++2c" "c++23" "c++2b" "c++20"; do
	dnl cov-scan cannot take too high a standard
	AS_IF([test -n "$COVERITY" && test "$i" != "c++20"], [continue])
	AC_MSG_CHECKING([if $CXX${i:+ -std=$i} works for our code])
	CXXFLAGS="$saved_CXXFLAGS${i:+ -std=$i}"
	AC_COMPILE_IFELSE([AC_LANG_SOURCE([
		#include <span>
		#include <unordered_map>
		#include <vector>
		int main() {
			std::vector<int> v{1,9};
			std::span s = v;
			std::unordered_map<int, int> um{{}};
		}
	])], [
		AC_MSG_RESULT([yes])
		cxxmode="${i:+ -std=$i}"
		break
	], [
		AC_MSG_RESULT([no])
	])
done
CXXFLAGS="$saved_CXXFLAGS"
AS_IF([test "$cxxmode" = error], [
	AC_MSG_ERROR([None of the -std= flags we tried led to successful compilation, but we need C++20 support.])
])
my_CXXFLAGS="$my_CXXFLAGS${cxxmode}"

AM_CONDITIONAL([ENABLE_PRIVATE_HEADERS], [test "$install_headers" = yes])
AC_ARG_WITH([asan], AS_HELP_STRING([--with-asan], [Activate Address Sanitizer]),
	[
	my_CFLAGS="$my_CFLAGS -fsanitize=address"
	my_CXXFLAGS="$my_CXXFLAGS -fsanitize=address"
	my_LDFLAGS="$my_LDFLAGS -fsanitize=address"
	])
dnl fvis is non-conforming and (rightfully) triggers ubsan warnings;
dnl Only attempt visibility/ODR tricks when sanitizers are deactivated
dnl and when a good linker is available.
NO_VSYM=""
AC_ARG_WITH([ubsan], AS_HELP_STRING([--with-ubsan], [Activate Undefined Behavior Sanitizer]), [
	my_CFLAGS="$my_CFLAGS -fsanitize=undefined"
	my_CXXFLAGS="$my_CXXFLAGS -fsanitize=undefined"
	my_LDFLAGS="$my_LDFLAGS -fsanitize=undefined"
	NO_VSYM=1
])
AS_IF([test -z "$NO_VSYM"], [
	my_CFLAGS="$my_CFLAGS -fvisibility=hidden"
	my_CXXFLAGS="$my_CXXFLAGS -fvisibility=hidden -fvisibility-inlines-hidden"
])
AS_IF([test "$with_gnu_ld" != yes], [NO_VSYM=1])
AC_SUBST([NO_VSYM])


PKG_CHECK_MODULES([libHX], [libHX >= 5.3])
PKG_CHECK_MODULES([sqlite], [sqlite3])

AC_SUBST([my_CPPFLAGS])
AC_SUBST([my_CFLAGS])
AC_SUBST([my_CXXFLAGS])
AC_SUBST([my_LDFLAGS])

AS_IF([test -z "$cxxmode"], [AC_MSG_RESULT([*** No usable -std= argument was detected for this C++ compiler.])])
AC_LANG_POP([C++])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
