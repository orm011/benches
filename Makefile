CXXWARNS=-Wall -Wextra -Wno-unused-parameter -Werror -Wno-macro-redefined -Wno-char-subscripts
CXXFLAGS=--std=c++14 -march=native -mtune=native -O3 -g $(CXXWARNS)
CILKFLAGS=-I$(CILKPUB) -fcilkplus

tpch: tpch.cc
	$(CXX) $(CXXFLAGS) $(CILKFLAGS) tpch.cc -o tpch

mymemcpy: mymemcpy.cc
	$(CXX) $(CXXFLAGS) mymemcpy.cc -o mymemcpy

clean:
	rm -f *~ a.out tpch mymemcpy
