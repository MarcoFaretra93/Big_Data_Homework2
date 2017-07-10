import argparse
import subprocess
import os

parser = argparse.ArgumentParser()

parser.add_argument("action", help="the action that needs to be invoked", choices=["all", "populate", "2_point_shooters", "3_point_shooters","attackers","defenders","rebounders","plus_minus", "all_around"])
parser.add_argument("-ip", "--master-ip", help="ip address of the driver/master, could be a name resolvable with DNS")
parser.add_argument("-dist", "--distributed", action="store_true", help="switch to cluster mode")
parser.add_argument("-c", "--college", action="store_true", help="switch to college analysis for category 'action'")
parser.add_argument("-dp", "--data-provider", help="choose the data provider used during the worker parallelization. Redis is very slow", choices=["mongo", "redis"])
parser.add_argument("-l", "--limit", help="choose the number of record parallelized at once, reducing ram usage but increasing network usage. Used only if the 'data-provider' is redis. default alphabetical splitting", default=0, type=int)

args = parser.parse_args()
os.environ["PYTHONPATH"] = "mongo-hadoop/spark/src/main/python"

if(args.action == "populate"):
	#subprocess.check_call(["python estrattore.py all delete -1 " + args.master_ip], shell=True)
	subprocess.check_call(["spark-submit main.py populate -ip " + args.master_ip], shell=True)
elif(args.action == "all"):
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + 'attackers' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + 'defenders' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + 'plus_minus' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + '3_point_shooters' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + '2_point_shooters' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + 'all_around' ], shell=True)
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + 'rebounders' ], shell=True)
else:
	subprocess.check_call(["spark-submit --driver-memory 6g --jars \"dependencies/mongo-hadoop-core-2.0.2.jar,dependencies/mongo-hadoop-spark-2.0.2.jar,dependencies/mongodb-driver-3.4.2.jar\" main.py -ip " + args.master_ip + " -dp " + args.data_provider + (' -c ' if args.college else ' ') + (' --dist ' if args.distributed else ' ') + (' --limit ' + args.limit if args.limit else ' ') + args.action ], shell=True)