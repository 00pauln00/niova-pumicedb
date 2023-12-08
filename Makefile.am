noinst_LTLIBRARIES =
noinst_PROGRAMS =
TESTS =
lib_LTLIBRARIES =
libexec_PROGRAMS =
pkgconfig_DATA = ./niova.pc
# libexec installs are placed into the 'niova' parent directory
my_libexecdir = $(libexecdir)/niova
my_libexec_PROGRAMS =

my_includedir = $(includedir)/niova
my_include_HEADERS =

ACLOCAL_AMFLAGS = -I m4

## Pumice
lib_LTLIBRARIES += src/libniova_pumice.la
src_libniova_pumice_la_LDFLAGS = \
	-version-info @MAJOR_VERSION@:@MINOR_VERSION@:@VERSION_SUFFIX@
src_libniova_pumice_la_LIBADD = \
	$(NIOVA_RAFT_LIBS) $(NIOVA_PUMICE) $(ROCKSDB_LIBS)
src_libniova_pumice_la_SOURCES = \
	src/include/pumice_db.h src/include/pumice_db_net.h src/pumice_db.c

lib_LTLIBRARIES += src/libniova_pumice_client.la
src_libniova_pumice_client_la_LDFLAGS = \
	-version-info @MAJOR_VERSION@:@MINOR_VERSION@:@VERSION_SUFFIX@
src_libniova_pumice_client_la_LIBADD = \
	$(NIOVA_RAFT_CLIENT_LIBS) $(NIOVA_PUMICE_CLIENT_LIBS)
src_libniova_pumice_client_la_SOURCES = \
	src/include/pumice_db_net.h \
	src/include/pumice_db_client.h \
	src/pumice_db_client.c

my_libexec_PROGRAMS += test/pumice-reference-client
test_pumice_reference_client_SOURCES =  \
	test/pumice-reference-client.c
test_pumice_reference_client_LDADD = \
	$(NIOVA_RAFT_CLIENT_LIBS) $(NIOVA_PUMICE_CLIENT_LIBS)

my_libexec_PROGRAMS += test/pumice-reference-server
test_pumice_reference_server_SOURCES =  \
	test/pumice-reference-server.c
test_pumice_reference_server_LDADD = \
	$(ROCKSDB_LIBS) $(NIOVA_RAFT_LIBS) $(NIOVA_PUMICE)

libniova_pumice_includedir = $(includedir)/niova/
libniova_pumice_include_HEADERS = src/include/pumice_db_net.h \
	src/include/pumice_db_client.h \
	src/include/pumice_db.h

autofmt:
	uncrustify -c tools/uncrustify.cfg --no-backup `find . -name "*.[ch]"` | tee /dev/null

autofmt-check:
	uncrustify -c tools/uncrustify.cfg -q --check `find . -name "*.[ch]"` | grep -c FAIL | grep ^0$ > /dev/null

version:
	echo "Version info: major=@MAJOR_VERSION@ minor=@MINOR_VERSION@ suffix=@VERSION_SUFFIX@"

show-flags:
	echo "AM_CFLAGS: ${AM_CFLAGS}"
	echo "CFLAGS: ${CFLAGS}"

gen-coverage:
	lcov --no-external -b . --capture --directory . \
		--output-file niova-coverage.out
	genhtml ./niova-coverage.out --output-directory ./niova-coverage
