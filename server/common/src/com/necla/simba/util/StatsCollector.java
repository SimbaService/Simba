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
/**
 * 
 */
package com.necla.simba.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records the entry/exit times for a message.
 */
public class StatsCollector {
	private PrintWriter pw;
	private HashMap<Integer, Long> pending;
	private HashMap<Integer, Double> totals;
	private static final Logger LOG = LoggerFactory
			.getLogger(StatsCollector.class);

	public void start(String filename) {
		try {
			pw = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
		} catch (IOException e) {
			e.printStackTrace();
			pw = null;
		}

		pending = new HashMap<Integer, Long>();
		totals = new HashMap<Integer, Double>();

		// flush the print buffer every N seconds (N = 30)
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				// LOG.debug("Flushing print buffer...");
				pw.flush();
			}
		}, 0, 10000);
	}

	public void write(String line) {
		if (pw != null) {
			pw.println(line);
		}
	}

	public void add(Integer id, boolean write) {
		if (pending.containsKey(id)) {
			// exit stamp
			Double total = (System.nanoTime() - (double) pending.get(id)) / 1000000;
			pending.remove(id);

			if (totals.containsKey(id)) {
				// already recorded a time for this sequence number
				Double sum = totals.get(id);
				sum += total;

				if (pw != null && write == true) {
					pw.println(sum.toString());

					totals.remove(id);
				}

			} else {
				// first time seeing this sequence number
				totals.put(id, total);

				if (pw != null && write == true) {
					pw.println(total.toString());

					totals.remove(id);
				}
			}
		} else {
			// entry stamp
			pending.put(id, System.nanoTime());
		}
	}
}
