CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

OBJS = sqlshell.o

%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

clean:
	rm -f sqlshell *.o
