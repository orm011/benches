CXXWARNS=-Wall -Wextra -Wno-unused-parameter -Werror -Wno-char-subscripts -Wno-deprecated-declarations
LDDFLAGS=-lboost_program_options

CXXFLAGS=--std=c++14 -march=native -mtune=native -O3 -g $(CXXWARNS) -fcilkplus
HEADERS=common.h

%.bin: %.cc $(HEADERS)
	$(CXX) $(CXXFLAGS) $(AVX) -I$(HOME)/cilkpub_v106/include $(LDDFLAGS) $< -o $@

%.istc.bin: %.cc $(HEADERS)
	g++-5.1.0 -I/home/orm/boost_1_59_0/ -I/home/orm/cilkpub_v106/include $(CXXFLAGS) -L/home/orm/boost_1_59_0/stage/lib/ -Wl,-rpath=/home/orm/boost_1_59_0/stage/lib/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../../lib64/:/lib/x86_64-linux-gnu/:/lib/../lib64/:/usr/lib/x86_64-linux-gnu/:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.1.0/../../../:/lib/:/usr/lib/ $(LDDFLAGS) $< -o $@

TOOLS=$(HOME)/tools
%.lanka.bin: %.cc $(HEADERS)
	$(TOOLS)/gcc-install/bin/g++ -I"$(TOOLS)/boost-install/include/" -I"$(TOOLS)/cilkpub_v106/include" -L"$(TOOLS)/boost-install/lib/"  -Wl,-rpath="$(TOOLS)/boost-install/lib/:$(TOOLS)/gcc-install/lib64/" $(CXXFLAGS) $(LDDFLAGS) $< -o $@

clean:
	rm -f *~ *.bin
