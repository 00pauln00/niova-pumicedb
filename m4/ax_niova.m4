AC_DEFUN([AX_NIOVA], [

CFLAGS_OLD=$CFLAGS
CPPFLAGS_OLD=$CPPFLAGS
LDFLAGS_OLD=$LDFLAGS

# NIOVA
AC_ARG_WITH([niova],
        [AS_HELP_STRING([--with-niova], [path to NIOVA library installation
            @<:@default=check@:>@])],
        [NIOVA_PATH=${withval}],
        [with_niova=check])

AS_IF([test "x$with_niova" = xno],
      [NIOVA_CFLAGS=""
       NIOVA_CPPFLAGS=""
       NIOVA_LDFLAGS=""],
      [test "x$with_niova" = xcheck],
      [NIOVA_CFLAGS=""
       NIOVA_CPPFLAGS=""
       NIOVA_LDFLAGS=""],
      [NIOVA_CFLAGS=""
       NIOVA_CPPFLAGS="-I${NIOVA_PATH}/include -I${NIOVA_PATH}/include/niova"
       NIOVA_LDFLAGS="-L${NIOVA_PATH}/lib"])

AC_SUBST([NIOVA_CFLAGS])
AC_SUBST([NIOVA_CPPFLAGS])
AC_SUBST([NIOVA_LDFLAGS])

CFLAGS="$CFLAGS $NIOVA_CFLAGS"
CPPFLAGS="$CPPFLAGS $NIOVA_CPPFLAGS"
LDFLAGS="$LDFLAGS $NIOVA_LDFLAGS"

$2
AS_IF([test "x$with_niova" != xno],
        [AC_CHECK_LIB([niova],[ctl_svc_node_get],[AC_SUBST([NIOVA_LIBS],["-lniova -ldl -Wl,-rpath -Wl,${NIOVA_PATH}/lib"])
            AC_CHECK_HEADERS([niova/common.h], [], [AC_MSG_ERROR([could not find niova/common.h])], [])
            $1
            AC_DEFINE([HAVE_NIOVA],[1],[Define if NIOVA detected])],
        [if test "x$with_niova" != xcheck; then
            AC_MSG_FAILURE([niova test failed])
        fi], )])

#echo "ax-m4 cppflags='$CPPFLAGS_OLD'"

CFLAGS=$CFLAGS_OLD
CPPFLAGS=$CPPFLAGS_OLD
LDFLAGS=$LDFLAGS_OLD
])
