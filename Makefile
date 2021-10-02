CXX = g++-11
CXXFLAGS = -Wall -std=gnu++20 -g -fsanitize=leak

SRCS   = $(wildcard src/*.cpp)
OBJS   = $(SRCS:.cpp=.o)
BASESRCS = $(filter-out %test.cpp %main.cpp,$(SRCS))
BASEOBJS = $(BASESRCS:.cpp=.o)

DBSRCS = $(BASESRCS)
DBSRCS += src/main.cpp
DBOBJS = $(DBSRCS:.cpp=.o)
TESTSRCS = $(BASESRCS) 
TESTSRCS += src/test.cpp
TESTOBJS = $(TESTSRCS:.cpp=.o)
CTESTSRCS = $(BASESRCS)
CTESTSRCS += src/crash_test.cpp
CTESTOBJS = $(CTESTSRCS:.cpp=.o)

all: mydb test crash_test

mydb: $(DBOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

test: $(TESTOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

crash_test: $(CTESTOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

# examples/*.cpp
%: $(BASEOBJS) examples/%.o
	$(CXX) $(CXXFLAGS) -o $@ $^
	
%.o: src/%.cpp
	$(CXX) -c $(CXXFLAGS) -o $@ $<

clean:
	rm mydb test crash_test example1 example2 example3 $(OBJS) examples/*.o

.PHONY: clean all