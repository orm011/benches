CXXWARNS=-Wall -Wextra -Wno-unused-parameter -Werror -Wno-macro-redefined -Wno-char-subscripts -Wno-deprecated-declarations
LDDFLAGS=-lboost_program_options
CXXFLAGS=--std=c++14 -march=native -mtune=native -O3 -g $(CXXWARNS) -fcilkplus
CILKFLAGS=-I$(CILKPUB)

tpch: tpch.cc
	$(CXX) $(CXXFLAGS) $(CILKFLAGS) $(LDDFLAGS) tpch.cc -o tpch.bin

mymemcpy: mymemcpy.cc
	$(CXX) $(CXXFLAGS) $(LDDFLAGS) mymemcpy.cc -o mymemcpy.bin

clean:
	rm -f *~ *.bin

istcmmymemcpy: mymemcpy.cc
	g++-5.1.0 -I/home/orm/boost_1_59_0/ -I/home/orm/cilkpub_v106/include --std=c++14 -march=native -mtune=native -O3 -g -Wall -Wextra -Wno-unused-parameter -Werror -Wno-char-subscripts -L/home/orm/boost_1_59_0/stage/lib/ -Wno-deprecated-declarations -Wl,-rpath=/home/orm/boost_1_59_0/stage/lib/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../../lib64/:/lib/x86_64-linux-gnu/:/lib/../lib64/:/usr/lib/x86_64-linux-gnu/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../:/lib/:/usr/lib/ -fcilkplus -lboost_program_options mymemcpy.cc -o mymemcpy.bin

istctpch:  tpch.cc
	g++-5.1.0 -I/home/orm/boost_1_59_0/ -I/home/orm/cilkpub_v106/include --std=c++14 -march=native -mtune=native -O3 -g -Wall -Wextra -Wno-unused-parameter -Werror -Wno-char-subscripts -L/home/orm/boost_1_59_0/stage/lib/ -Wno-deprecated-declarations -Wl,-rpath=/home/orm/boost_1_59_0/stage/lib/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../../lib64/:/lib/x86_64-linux-gnu/:/lib/../lib64/:/usr/lib/x86_64-linux-gnu/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../:/lib/:/usr/lib/ -fcilkplus -lboost_program_options tpch.cc -o tpch.bin

TOOLS=$(HOME)/tools
%.lanka.bin: %.cc
	$(TOOLS)/gcc-install/bin/g++ -I"$(TOOLS)/boost-install/include/" -I"$(TOOLS)/cilkpub_v106/include" --std=c++14 -march=native -mtune=native -O3 -g -Wall -Wextra -Wno-unused-parameter -Werror -Wno-char-subscripts -L"$(TOOLS)/boost-install/lib/"  -Wl,-rpath="$(TOOLS)/boost-install/lib/:$(TOOLS)/gcc-install/lib64/" --std=c++14 -march=corei7-avx -mtune=corei7-avx -g $(CXXWARNS) -fcilkplus $(LDDFLAGS) $< -o $@
