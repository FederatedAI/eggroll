import os 
import time 
import argparse 

arg_parser = argparse.ArgumentParser() 
arg_parser.add_argument("-t","--time", type=int, help="Sleep time wait, default value 0s", default=0) 
args = arg_parser.parse_args() 

if args.time == 0: 
	os.system('sh ./cluster_env_check.sh')
else: 
	while 1: 
		os.system('sh ./cluster_env_check.sh')
		time.sleep(args.time) 
