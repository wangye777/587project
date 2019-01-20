cd..

	make (..will generate base and skipv)

e.g. if adopt ParallelDPEnum method, join 12 tables using 4 threads, Star query

	./base 12 4 Star
e.g. if adopt ParallelDPEnumwithSV method, join 12 tables using 3 threads, Linear query

	./skipv 12 3 Linear
e.g. if adopt ParallelDPEnumwithSV method, join 12 tables using 2 threads, Clique query

	./skipv 12 2 Clique
	
make sure the number of threads is no more than number of cores 
