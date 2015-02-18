import os
import time
import sys
import tempfile

DATA_KEYSPACE="test_3replicas"
#CONSISTENCY_LEVEL="STRONG"
CONSISTENCY_LEVEL="CAUSAL"
#CONSISTENCY_LEVEL="EVENTUAL"

class CreateCassandraTables:

	def _generate_table_list(self, prefix, num_tables, num_cols, num_objs, start_tbl):
		tables =""
		for n in range(start_tbl, start_tbl+num_tables):
			tablename = prefix+str(n)
			create = "create table "+DATA_KEYSPACE+"."+tablename
			key = " (key text PRIMARY KEY"
			cols = ""
			for i in range(0, num_cols):
				cols += ", col"+str(i)+ " text"
			deleted = ", deleted boolean"
			#objs = ""
			#for i in range(0, num_objs):
			#	objs += ", obj"+str(i)+" list<uuid>"
			objs = ", large list<uuid>"
			version = ", version int);"
			cmd = create+key+cols+deleted+objs+version+"\n"
			print cmd
			tables+=cmd
			vindex = "create index "+tablename+"_idx on "+DATA_KEYSPACE+"."+tablename+" (version);\n"
			print vindex
			tables += vindex
			cmd_md = "insert into simbastore.metadata (key, consistency) values ('"+DATA_KEYSPACE+"."+tablename+"', '"+CONSISTENCY_LEVEL+"');"
			tables += cmd_md
		return tables
	
	def _initialize_db(self, prefix, num_tables, num_cols, num_objs, start_tbl):
		tables = self._generate_table_list(prefix, num_tables, num_cols, num_objs, start_tbl)
                print "initializing db"
                fh, path = tempfile.mkstemp()
                with open(path,'w') as outfile:
                        outfile.write(tables)
                os.close(fh)
                os.system("/opt/apache-cassandra-1.2.5/bin/cqlsh -f %s" % path)
                os.unlink(path)

	def test(self, prefix, num_tables, num_cols, num_objs, start_tbl):
		self._generate_table_list(prefix, num_tables, num_cols, num_objs, start_tbl)
	
	def create(self, prefix, num_tables, num_cols, num_objs, start_tbl):
		self._initialize_db(prefix, num_tables, num_cols, num_objs, start_tbl)


if __name__ == '__main__':
	prog = CreateCassandraTables()
        if len(sys.argv) > 1:
                if sys.argv[1] == "test":
			prog.test(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
                elif sys.argv[1] == "create":
			prog.create(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))
			
