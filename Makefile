release: tpch.cc
	g++ --std=c++14 -Wall -Wextra -Wno-unused-parameter -Werror -fcilkplus -O2 -g -Wno-macro-redefined tpch.cc -DITEMS=100000000 -Wno-char-subscripts
