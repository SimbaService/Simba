#!/usr/bin/env python

from sys import exit,argv
import random

if len(argv) < 13:
	print "Usage:"
	print argv[0] + " <# tables> <# rows> <obj size> <start row> <start version> <writers per table> <readers per table> <# client hosts> <output prefix> <table prefix>"
	print
	exit(1)

tables = int(argv[1])
num_rows = int(argv[2])
object_size = argv[3]
start_row = int(argv[4])
start_version = int(argv[5])
writers_per_table = int(argv[6])
readers_per_table = int(argv[7])
nodes = int(argv[8])
output_prefix = argv[9]
TABLE_PREFIX = argv[10]
read_start_delay_minutes = argv[11]
write_start_delay_minutes = argv[12]

# set delay tolerance in milliseconds
READ_DT = "0"

# set target operations per second
#OPS_PER_SECOND = 100.0
#OPS_PER_SECOND = 300.0
OPS_PER_SECOND = 500.0
#OPS_PER_SECOND = 700.0
#OPS_PER_SECOND = 900.0

# this value determines how many "read periods" to choose from when creating readers
READ_STAGGER_FACTOR=20

# this value determines the maximum percentage into the "write delay window" to allow a read period
MAX_READ_DELAY_PERCENTAGE=0.75

WRITE_DELAY = str(long((((writers_per_table + (readers_per_table * writers_per_table)) * tables) / OPS_PER_SECOND) * 1000))
READ_PERIOD_STEP = int(int(WRITE_DELAY)/READ_STAGGER_FACTOR)



t = 0
write_subs = []
read_subs =[]

for i in range(0, tables):
	#READERS        
	if readers_per_table > 0:
		for j in range(readers_per_table):
			read_period = random.choice(range(READ_PERIOD_STEP,int(int(WRITE_DELAY)*MAX_READ_DELAY_PERCENTAGE),READ_PERIOD_STEP))
			#read_period = 1000
			s = ["R", TABLE_PREFIX + str(t), str(read_period), READ_DT, str(start_version), str(writers_per_table * num_rows), read_start_delay_minutes, "1"]
			read_subs.append(' '.join(s) + '\n')

	#WRITERS
	if writers_per_table > 0:       
		for j in range(writers_per_table):
			s = ["W", TABLE_PREFIX + str(t), str(num_rows), str(start_row + (j * num_rows)), str(start_version), object_size, WRITE_DELAY, read_start_delay_minutes, write_start_delay_minutes, "1"]
			write_subs.append(' '.join(s) + '\n')
	t += 1

#merge subscription lists
subs = []
subs.extend(write_subs)
subs.extend(read_subs)

#create map for individual node lists
node_subs = {k: [] for k in range(nodes)}

node = 0
for i in range(0, len(subs)):
	node_subs[node].append(subs[i])	
	
	#select next node
	node = (node + 1) % nodes

for k,v in node_subs.items():
	filename = output_prefix + "." + str(k+1)
	print "creating file: " + filename
	with open(filename, 'w') as outfile:
		for thing in v:
			outfile.write(thing)

