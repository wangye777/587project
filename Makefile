all: ParallelDPEnum.cpp ParallelDPEnumwithSV.cpp
	g++ -pthread ParallelDPEnum.cpp -o base -std=c++11
	g++ -pthread ParallelDPEnumwithSV.cpp -o skipv -std=c++11

