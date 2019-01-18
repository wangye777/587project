cd..
make (..will generate base and skipv)

e.g. if adopt ParallelDPEnum method, join 13 tables using 4 threads, Star query
	./base 13 4 Star
e.g. if adopt ParallelDPEnumwithSV method, join 14 tables using 2 threads, Linear query
	./skipv 14 2 Linear
e.g. if adopt ParallelDPEnumwithSV method, join 14 tables using 2 threads, Clique query
	./skipv 14 2 Clique
	
make sure the number of threads is no more than number of cores 
