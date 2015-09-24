tpch: tpch.cc
	$(CXX) --std=c++14 -Wall -Wextra -Wno-unused-parameter -Werror -fcilkplus -O2 -g -Wno-macro-redefined tpch.cc -DITEMS=100000000 -Wno-char-subscripts -o tpch
clean:
	rm -f *~ a.out tpch
