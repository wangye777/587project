all: ParallelDPEnum.cpp ParallelDPEnumwithSV.cpp
	g++ -pthread -O3 ParallelDPEnum.cpp -o base -std=c++11
	g++ -pthread -O3 ParallelDPEnumwithSV.cpp -o skipv -std=c++11

