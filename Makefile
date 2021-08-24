CXX = g++
CXXFLAGS = -Wall -std=gnu++17
SRCS   = $(wildcard src/*.cpp)
OBJS   = $(SRCS:.cpp=.o)
DBSRCS = $(filter-out %test.cpp,$(SRCS))
DBOBJS = $(DBSRCS:.cpp=.o)
TESTSRCS = $(filter-out %crash_test.cpp %main.cpp,$(SRCS))
TESTOBJS = $(TESTSRCS:.cpp=.o)
CTESTSRCS = $(filter-out %main.cpp,$(DBSRCS))
CTESTSRCS += src/crash_test.cpp
CTESTOBJS = $(CTESTSRCS:.cpp=.o)

all: mydb test

mydb: $(DBOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

test: $(TESTOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

crash_test: $(CTESTOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: src/%.cpp
	$(CXX) -c $(CXXFLAGS) -o $@ $<

clean:
	rm mydb test crash_test $(OBJS) 

.PHONY: clean all