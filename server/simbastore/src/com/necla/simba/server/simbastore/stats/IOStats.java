/*******************************************************************************
 *   Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
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
package com.necla.simba.server.simbastore.stats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;

public class IOStats {
	private static PrintWriter pw;
	private static boolean running = false;

	private static volatile long numGetRows;
	private static volatile long numGetRowsByVersion;
	private static volatile long numPutRows;
	private static volatile long numDelRows;
	private static volatile long numMarkDelRows;
	private static volatile long numGetObject;
	private static volatile long numPutObject;
	private static volatile long numDelObject;

	private static volatile double timeGetRows;
	private static volatile double timeGetRowsByVersion;
	private static volatile double timePutRows;
	private static volatile double timeDelRows;
	private static volatile double timeMarkDelRows;
	private static volatile double timeGetObject;
	private static volatile double timePutObject;
	private static volatile double timeDelObject;

	public static void start() {
		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(
					"io_stats.csv", false)));
			running = true;
		} catch (IOException e) {
			e.printStackTrace();
			pw = null;
		}
		// flush the print buffer every N seconds (N = 30)
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				// LOG.debug("Flushing print buffer...");
				System.out.println(getData());
			}
		}, 0, 30000);
	}

	public static void stop() {
		if (running && pw != null) {
			pw.close();
			running = false;
		}
	}

	public static String getData() {
		StringBuffer sb = new StringBuffer();
		sb.append("=================================\n");
		sb.append("num_get_rows=").append(numGetRows);
		sb.append("\nnum_get_rows_by_version=").append(numGetRowsByVersion);
		sb.append("\nnum_put_rows=").append(numPutRows);
		sb.append("\nnum_del_rows=").append(numDelRows);
		sb.append("\nnum_mark_del_rows=").append(numMarkDelRows);
		sb.append("\nnum_get_obj=").append(numGetObject);
		sb.append("\nnum_put_obj=").append(numPutObject);
		sb.append("\nnum_del_object=").append(numDelObject);
		
		sb.append("\ntime_get_rows=").append(timeGetRows);
		sb.append("\ntime_get_rows_by_version=").append(timeGetRowsByVersion);
		sb.append("\ntime_put_rows=").append(timePutRows);
		sb.append("\ntime_del_rows=").append(timeDelRows);
		sb.append("\ntime_mark_del_rows=").append(timeMarkDelRows);
		sb.append("\ntime_get_obj=").append(timeGetObject);
		sb.append("\ntime_put_obj=").append(timePutObject);
		sb.append("\ntime_del_object=").append(timeDelObject);
		sb.append("\n=================================");

		if (running && pw != null) {
			pw.flush();
		}
		return sb.toString();
	}
	
	private static void log(String event, double latency) {
		if (running && pw != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(System.currentTimeMillis()).append(",").append(event)
					.append(",").append(latency);
			pw.println(sb.toString());
		}
	}

	public static void getRow(double latency) {
		if (latency > 0) {
			++numGetRows;
			timeGetRows += latency;
			log("gr", latency);
		}
	}
	
	public static void getRowByVersion(double latency) {
		if (latency > 0) {
			++numGetRowsByVersion;
			timeGetRowsByVersion += latency;
			log("grv", latency);
		}
	}

	public static void putRow(double latency) {
		if (latency > 0) {
			++numPutRows;
			timePutRows += latency;
			log("pr", latency);
		}
	}

	public static void delRow(double latency) {
		if (latency > 0) {
			++numDelRows;
			timeDelRows += latency;
			log("dr", latency);
		}
	}
	
	public static void markDelRow(double latency) {
		if (latency > 0) {
			++numMarkDelRows;
			timeMarkDelRows += latency;
			log("mdr", latency);
		}
	}

	public static void getObject(double latency) {
		if (latency > 0) {
			++numGetObject;
			timeGetObject += latency;
			log("go", latency);
		}
	}

	public static void putObject(double latency) {
		if (latency > 0) {
			++numPutObject;
			timePutObject += latency;
			log("po", latency);
		}
	}

	public static void delObject(double latency) {
		if (latency > 0) {
			++numDelObject;
			timeDelObject += latency;
			log("do", latency);
		}
	}
}
