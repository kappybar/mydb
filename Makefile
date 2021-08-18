CXX = g++
CXXFLAGS = -Wall -std=gnu++17
SRCS   = $(wildcard src/*.cpp)
OBJS   = $(SRCS:.cpp=.o)
DBSRCS = $(filter-out %test.cpp,$(SRCS))
DBOBJS = $(DBSRCS:.cpp=.o)
TESTSRCS = $(filter-out %main.cpp,$(SRCS))
TESTOBJS = $(TESTSRCS:.cpp=.o)

all: mydb test

mydb: $(DBOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

test: $(TESTOBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm mydb test $(OBJS) 

.PHONY: clean all