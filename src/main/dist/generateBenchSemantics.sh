#!/bin/bash

for PC in 2848 28480 284800
do
	for SER in nqr nqp trig ttl
	do
		DIR="./data/10gc/$PC/$SER/"
		mkdir -p $DIR
		./generate -pc $PC -s $SER 
		if [ $SER = "trig" ]; then
			./generateAuth -fn dataset.trig -uc 50 -gc 10
			pigz dataset.trig
			pigz auth_session_mat.ttl
		elif [ $SER = "nqr" ] || [ $SER = "nqp" ]; then
			./generateAuth -fn dataset.nq -uc 50 -gc 10
			pigz dataset.nq
			pigz auth_session_mat.ttl
		else
			pigz dataset.ttl
		fi	
		
		mv dataset* $DIR 
		mv  *.list  $DIR 
		mv auth* $DIR 
		mv td_data/ $DIR 
	done
done