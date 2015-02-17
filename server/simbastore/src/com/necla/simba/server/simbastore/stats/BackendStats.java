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
import java.util.concurrent.ConcurrentHashMap;

import com.necla.simba.server.simbastore.stats.StatRecord;

public class BackendStats {
	private static final String delimiter = ",";
	private static ConcurrentHashMap<Integer, StatRecord> records;

	private static PrintWriter pw;
	private static boolean running = false;

	public static void start() {

		records = new ConcurrentHashMap<Integer, StatRecord>();

		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(
					"backend_stats.csv", false)));
			running = true;
		} catch (IOException e) {
			e.printStackTrace();
			pw = null;
		}

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				// LOG.debug("Flushing print buffer...");
				pw.flush();
			}
		}, 0, 30000);
	}

	public static void stop() {
		if (running && pw != null) {
			pw.close();
			running = false;
		}
	}

	public static void writeRecord(int tid) {
		StatRecord r = records.get(tid);
		if (r != null) {
			log(tid, r.cassandraReadLatency, r.cassandraReadBytes,
					r.cassandraWriteLatency, r.cassandraWriteBytes,
					r.swiftReadLatency, r.swiftReadBytes, r.swiftWriteLatency,
					r.swiftWriteBytes);
			records.remove(tid);
		}
	}

	public static void logCassandraReadLatency(int tid, double latency) {
		if (latency > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.cassandraReadLatency += latency;

			records.put(tid, r);
		}
	}

	public static void logCassandraWriteLatency(int tid, double latency) {
		if (latency > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.cassandraWriteLatency += latency;

			records.put(tid, r);
		}
	}

	public static void logCassandraReadBytes(int tid, long bytes) {
		if (bytes > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.cassandraReadBytes += bytes;
			records.put(tid, r);
		}
	}

	public static void logCassandraWriteBytes(int tid, long bytes) {
		if (bytes > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.cassandraWriteBytes += bytes;
			records.put(tid, r);
		}
	}

	public static void logSwiftReadLatency(int tid, double latency) {
		if (latency > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.swiftReadLatency += latency;
			records.put(tid, r);
		}
	}

	public static void logSwiftWriteLatency(int tid, double latency) {
		if (latency > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.swiftWriteLatency += latency;
			records.put(tid, r);
		}
	}

	public static void logSwiftReadBytes(int tid, long bytes) {
		if (bytes > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.swiftReadBytes += bytes;
			records.put(tid, r);
		}
	}

	public static void logSwiftWriteBytes(int tid, long bytes) {
		if (bytes > 0) {
			StatRecord r;
			r = records.get(tid);
			if (r == null) {
				r = new StatRecord();
			}
			r.swiftWriteBytes += bytes;
			records.put(tid, r);
		}
	}

	private static void log(int tid, double cassandraReadLatency,
			long cassandraReadBytes, double cassandraWriteLatency,
			long cassandraWriteBytes, double swiftReadLatency,
			long swiftReadBytes, double swiftWriteLatency, long swiftWriteBytes) {
		if (running && pw != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(System.currentTimeMillis()).append(delimiter).append(tid)
					.append(delimiter).append(cassandraReadLatency)
					.append(delimiter).append(cassandraReadBytes)
					.append(delimiter).append(cassandraWriteLatency)
					.append(delimiter).append(cassandraWriteBytes)
					.append(delimiter).append(swiftReadLatency)
					.append(delimiter).append(swiftReadBytes).append(delimiter)
					.append(swiftWriteLatency).append(delimiter)
					.append(swiftWriteBytes);

			pw.println(sb.toString());
		}
	}
}
