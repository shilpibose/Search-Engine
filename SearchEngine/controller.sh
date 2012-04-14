#!/bin/bash

if [ $# -ne 4 ]; then
	echo "Not enough arguments to start controller"
	exit
fi

export CLASSPATH=$CLASSPATH:lib/FreePastry-2.1.jar:lib/je-4.0.92.jar:lib/jtidy-r938.jar:lib/log4j-1.2.15.jar:bin
nohup java -cp $CLASSPATH edu.upenn.cis555.control.Controller $1 $2 $3 $4 < /dev/null > /dev/null 2>&1 &
