/*******************************************************************************
 *    Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.necla.simba.client;

import android.os.Environment;

/**
 * @category Global Preferences in Simba
 * @author nitin
 * 
 *
 */

public class Preferences {

	public static final String DBPATH =  Environment.getExternalStorageDirectory() + "/SCS/";

	public enum COMPRESS_CHOICE {
		COMPRESS_HEURISTIC_SIZE(0),
		COMPRESS_HEURISTIC_ENTROPY(1);
	
		private int statusCode;
		
		private COMPRESS_CHOICE(int i) {
			statusCode = i;
		}
	 	
		public int getStatusCode() {
			return statusCode;
		}
	}
	
	public static boolean WRITE_COMPRESS = true;
	public static boolean READ_COMPRESS = true;
	public static int COMPRESS_LEVEL = 9; // Best Compression
	public static int COMPRESS_HEURISTIC = COMPRESS_CHOICE.COMPRESS_HEURISTIC_SIZE.getStatusCode(); // pick one of size or entropy
	public static int COMPRESSABLE_SIZE = 100; // ensure 13 bytes notifications do not get get compressed
	public static boolean WAL = false; // WAL journal mode for app databases
	public static boolean PERSIST = true; // PERSIST journal mode for Simba DB
	
	public static final String SIMBA_DB_NAME = "simba.db";
	public static final String SIMBA_METADATA_TABLE = "metadata";
	
	public static final String NW_STATS_FILE = DBPATH + "nw.csv"; // for dumping network statistics
	
	public static int MAX_TRIM_LOOPS = 5; // for trimming partial tables
	public static int MAX_ENTRIES = 100; // for trimming partial tables

	public static int PIGGYBACK_MAX_SIZE = 1024 * 1024; // maximum size used in deciding piggybacking
	
	public static final String FILE_WALLET = DBPATH + "device.dat"; //wallet file

	public static final int RECONNECT_TIMER = 10000; // check network every 10s
	public static final int MAX_CONNECT_TIMER = 60000;
	public static final int DT_CONTROL_MSGS_PULL = 2000; // DT for control messages - passive pull
	public static final int DT_CONTROL_MSGS_ACK = 2000; // DT for control messages - notify ack
	
	public static final int READ_COMPRESS_BUFSIZE = 4096; // buffer to hold uncompressed data

	// SCS
	public static final String PREFS_NAME = "scs_preferences";
	public static final String DEFAULT_HOST = "138.15.110.55"; // storwire
	//public static final String DEFAULT_HOST = "138.15.166.60"; // Simba-test-vm
	public static final int DEFAULT_PORT = 9001;
	public static ConnState globalReadSyncPref = ConnState.TG;
	public static final double READ_DT_SERVER_RATIO = 0.8; // 80% of original DT is for server
	
	
	public Preferences() {
		// TODO Auto-generated constructor stub
	}

}
