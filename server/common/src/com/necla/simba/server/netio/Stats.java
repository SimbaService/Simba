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
package com.necla.simba.server.netio;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats {
	private static final Logger LOG = LoggerFactory.getLogger(Stats.class
			.getName());

	private static volatile long numBytesSent;
	private static volatile long numMessagesSent;
	private static volatile long numBytesReceived;
	private static volatile long numMessagesReceived;
	private static volatile long numMessagesQueued;
	private static volatile long numBytesQueued;
	private static volatile long numMessagesDequeued;

	public static volatile Long numBytesSentPerSecond = 0L;
	public static volatile Long numBytesReceivedPerSecond = 0L;

	public static int t = 0;

	private static PrintWriter pw;
	private static PrintWriter pw_tput;

	private static boolean running = false;

	public static void start() {
		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(
					"network-stat.csv", false)));
			running = true;
		} catch (IOException e) {
			e.printStackTrace();
			pw = null;
		}

		try {
			pw_tput = new PrintWriter(new BufferedWriter(new FileWriter(
					"throughput.csv", false)));
		} catch (IOException e) {
			e.printStackTrace();
			pw_tput = null;
		}

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				// LOG.debug("Flushing print buffer...");
				System.out.println(getData());
			}
		}, 0, 30000);

		long period = 1000;
		long now = System.currentTimeMillis();
		now = ((now + period - 1) / period) * period;

		Timer tput_calc = new Timer();
		tput_calc.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				if (pw_tput != null) {

					StringBuffer sb = new StringBuffer();
					sb.append(System.currentTimeMillis());

					synchronized (numBytesReceivedPerSecond) {
						sb.append(",").append(numBytesReceivedPerSecond);
						numBytesReceivedPerSecond = (long) 0;
					}

					synchronized (numBytesSentPerSecond) {
						sb.append(",").append(numBytesSentPerSecond);
						numBytesSentPerSecond = (long) 0;
					}

					pw_tput.println(sb.toString());
				}

				if (t == 10) {
					pw_tput.flush();
					t = 0;
				} else {
					t ++;
				}

			}
		}, new Date(now), period);

	}

	public static void stop() {
		if (running && pw != null) {
			pw.close();
			running = false;
		}
		pw_tput.close();
	}

	private static void log(String event, int count) {
		if (running && pw != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(System.currentTimeMillis()).append(",").append(event)
					.append(",").append(count);
			pw.println(sb.toString());
		}
	}

	public static String getData() {
		StringBuffer sb = new StringBuffer();

		sb.append("=================================\n");
		sb.append("bytes_send=").append(numBytesSent);
		sb.append(" messages_send=").append(numMessagesSent);
		sb.append(" bytes_recv=").append(numBytesReceived);
		sb.append(" messages_recv=").append(numMessagesReceived);
		sb.append(" bytes_queued=").append(numBytesQueued);
		sb.append(" messages_queued=").append(numMessagesQueued);
		sb.append(" messages_dequeued=").append(numMessagesDequeued);
		sb.append("\n=================================");

		if (running && pw != null) {
			pw.flush();
		}
		return sb.toString();
	}

	public static void sent(int numBytes) {
		if (numBytes > 0) {

			synchronized (numBytesSentPerSecond) {
				numBytesSentPerSecond += numBytes;
			}

			++numMessagesSent;
			numBytesSent += numBytes;
			log("d", numBytes);
			LOG.debug("d: " + numBytes);

		}
	}

	public static void received(int numBytes) {
		if (numBytes > 0) {

			synchronized (numBytesReceivedPerSecond) {
				numBytesReceivedPerSecond += numBytes;
			}

			++numMessagesReceived;
			numBytesReceived += numBytes;
			log("u", numBytes);
			LOG.debug("u: " + numBytes);

		}
	}

	public static synchronized void queued(int numBytes) {
		++numMessagesQueued;
		numBytesQueued += numBytes;
		log("dq", numBytes);

	}

	public static synchronized void dequeued() {
		++numMessagesDequeued;
	}
}
