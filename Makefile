check : test/simple_test.c
	gcc -g -O0 -Isrc/include/ -o test/simple_test test/simple_test.c
	test/simple_test

pahole : check
	pahole test/simple_test

clean :
	rm -fv test/simple_test
