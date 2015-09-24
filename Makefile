ITEMS=100000

tpch: tpch.cc
	$(CXX) -I$(CILKPUB) --std=c++14 -Wall -Wextra -Wno-unused-parameter -Werror -fcilkplus -O2 -g -Wno-macro-redefined tpch.cc -DITEMS=$(ITEMS) -Wno-char-subscripts -o tpch

clean:
	rm -f *~ a.out tpch
