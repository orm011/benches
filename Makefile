ITEMS=100000

tpch: tpch.cc
	$(CXX) -I$(CILKPUB)  --std=c++14 -march=native -mtune=native -Wall -Wextra -Wno-unused-parameter -Werror -fcilkplus -O3 -g -Wno-macro-redefined tpch.cc -DITEMS=$(ITEMS) -Wno-char-subscripts -o tpch

clean:
	rm -f *~ a.out tpch
