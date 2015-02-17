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
package com.necla.simba.testclient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.necla.simba.client.SeqNumManager;
import com.necla.simba.server.netio.Stats;
import com.necla.simba.util.ConsistentHash;

public class Client {

	/**
	 * @param args
	 */

	static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
	static Random rnd = new Random();
	static SeqNumManager sequencer = new SeqNumManager();
	public static Properties properties;
	protected static ConsistentHash hasher;
	private static final int MEGABYTE = 1048576;
	private static final String KEYSPACE_NAME = "test_3replicas";

	public static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	public static void main(String[] args) throws InterruptedException {
		AtomicInteger waiting_to_connect = new AtomicInteger();

		String inputFile = null;
		String outdir = null;
		String outfile_prefix = null;

		List<Thread> threads = new LinkedList<Thread>();

		Stats.start();

		inputFile = args[0];
		outdir = args[1];
		outfile_prefix = args[2];

		String node_outfile_id = Integer.toString(rnd.nextInt());

		int readerCount = 0;
		int writerCount = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(inputFile));
			String line = br.readLine();

			while (line != null) {
				// split line
				String[] l = line.split(" ");

				if (l[0].equals("W")) {
					/*
					 * writer format: W <table name> <# rows to write> <starting
					 * row #> <starting version #> <object size> <delay b/t
					 * writes (ms)> <# writer clients>
					 */

					int startRowMultiplier = 0;

					for (int i = 0; i < Integer.parseInt(l[9]); i++) {
						// for multiple writers, increment the starting row
						// number by the number of total rows to write
						l[3] = Integer
								.toString(Integer.parseInt(l[3])
										+ (Integer.parseInt(l[2]) * startRowMultiplier));

						WriterThread w = new WriterThread(l,
								waiting_to_connect, outdir, outfile_prefix,
								node_outfile_id, writerCount);
						Thread t = new Thread(w);
						threads.add(t);
						startRowMultiplier++;
						writerCount++;
					}
				} else if (l[0].equals("R")) {
					/*
					 * reader format: R <table name> <subscription period (ms)>
					 * <delay tolerance interval (ms)> <start version> <num
					 * rows> <# reader clients>
					 */
					for (int i = 0; i < Integer.parseInt(l[7]); i++) {
						ReaderThread r = new ReaderThread(l,
								waiting_to_connect, outdir, outfile_prefix,
								readerCount, node_outfile_id);
						Thread t = new Thread(r);
						threads.add(t);
						readerCount++;
					}
				}

				// finally, read next line
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		waiting_to_connect.set(threads.size());

		for (Thread t : threads) {
			t.start();
		}

		// wait on threads and exit
		int exited = 0;
		for (Thread t : threads) {
			t.join();
			exited++;
			System.out.println("Thread stats: " + exited + " of "
					+ threads.size() + " joined/exited");
		}
		System.out.println("All client threads exited.");

		// sleep to ensure we see final network stats
		for (int i = 4; i > 0; i--) {
			System.out.println("Terminating in " + (i * 10) + " seconds");
			Thread.sleep(10 * 1000);
		}

		System.exit(0);
	}
}
