#!/bin/bash

if [ "populate" == $1 ] || [ "all" == $1 ]; then 
	python estrattore.py all delete -1
	export PYTHONPATH=mongo-hadoop/spark/src/main/python
	spark-submit main.py populate -ip localhost
fi 

if [ "all" == $1 ] || [ "run_spark" == $1 ]; then 
	if [ "-c" == $2 ]; then 
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c attackers
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c 2_point_shooters
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c 3_point_shooters
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c defenders
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c rebounders
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c plus_minus
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo -c all_around
	else 
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo attackers
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo 2_point_shooters
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo 3_point_shooters
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo defenders
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo rebounders
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo plus_minus
		spark-submit --driver-memory 6g --jars "dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar" main.py -ip localhost -dp mongo all_around
	fi 
fi 