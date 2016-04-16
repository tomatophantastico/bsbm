#!/bin/bash

DATA_DIR="."

for PC in 2848 
#28480 284800
do
	for SER in nqr nqp trig ttl
	do
		DIR=${DATA_DIR}/data/10gc/$PC/$SER/
		mkdir -p $DIR
		./generate -pc $PC -s $SER 
		if [ "$SER" == "trig" ]; then
			./generateAuth -fn dataset.trig -uc 20 -gc 10
			pigz -f dataset.trig
		elif [ "$SER" == "nqr" ] || [ "$SER" == "nqp" ]; then
			./generateAuth -fn dataset.nq -uc 20 -gc 10
			pigz -f dataset.nq
		else
			pigz -f dataset.ttl
		fi	
		
		mv dataset* $DIR 
		mv  *.list  $DIR 
		mv auth* $DIR 
		mv td_data/ $DIR
		mv virt* $DIR 
	done
done