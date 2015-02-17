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

import org.apache.log4j.Logger;

import android.util.Log;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/***
 * Utility class that logs network statistics to a file.
 */


public class SimbaLogger {
	private static Logger moblog;
	
	public enum Type {
		MMSG("Simba Message"),
		FIRST("FIRST Simba Message"),
		MMMSG("Simba Multi Message"),
		NOTIFY("Notification"),
		CTRL("Control Response"),
		FAIL("Failure Handling"),
		BYTES("Network Bytes"),
		START("Begin Logging"),
		END("End Logging"),
		PIG("Piggyback"),
		SCHED("Schedule"),
		SYNC_SEND("Sync send");
		
		private String statusCode;
		 
		private Type(String s) {
			statusCode = s;
		}
	 
		public String getStatusCode() {
			return statusCode;
		}
	}
	
	public enum Dir {
		UP("uplink"),
		DN("downlink"),
		LO("local");
		
		private String statusCode;
		 
		private Dir (String s) {
			statusCode = s;
		}
	 
		public String getStatusCode() {
			return statusCode;
		}
	}
	
	public static void start() {
		
		Log.v("SCS", "Turning ON Logging");
		//moblog = Logger.getLogger(SimbaLogger.class.getName());
		moblog = Logger.getLogger("SCS");
		moblog.setLevel(org.apache.log4j.Level.INFO);
	}
	
	public static void stop() {
		Log.v("SCS", "Turning OFF Logging");
		moblog.setLevel(org.apache.log4j.Level.OFF);
	}
	
	public static void log(Type lt, Dir dir, long size, Object obj) {
		if(moblog != null)
			moblog.info("" + lt.toString() + "," + dir.toString() + "," + size + "," + obj.toString());
			
	}
	
}
