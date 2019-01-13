all: main.cpp
	g++ -pthread -O3 main.cpp -o main -std=c++11
