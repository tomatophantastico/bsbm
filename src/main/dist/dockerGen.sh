#/bin/bash

for PC in 2848
do
	for SER in nqr nqp trig ttl
	do
		DIR="./data/10gc/$PC/$SER/"

		docker run -name run openlink-virtuoso-7


		if [ $SER = "trig" ]; then
			./generateAuth -fn dataset.trig -uc 50 -gc 10
			pigz dataset.trig
		elif [ $SER = "nqr" ] || [ $SER = "nqp" ]; then
			./generateAuth -fn dataset.nq -uc 50 -gc 10
			pigz dataset.nq
		else
			pigz dataset.ttl
		fi	
		
		mv dataset* $DIR 
		mv  *.list  $DIR 
		mv auth* $DIR 
		mv td_data/ $DIR 
	done
done