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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.necla.simba.client.SimbaNetworkManager;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Common.ColumnData;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncRequest;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.server.Preferences;
import com.necla.simba.util.ConsistentHash;
import com.necla.simba.util.Utils;

public class WriterThread implements Runnable {

	private static final int MINUTE = 60 * 1000;
	private static final int MEGABYTE = 1048576;
	private static final String KEYSPACE_NAME = "test_3replicas";
	private SeqNumManager sequencer = new SeqNumManager();

	private static final Logger LOG = LoggerFactory
			.getLogger(WriterThread.class);

	protected static Properties properties;
	protected static ConsistentHash hasher;

	static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

	private static final boolean RECORD_TIME = true;
	static Random rnd = new Random();

	public static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	AtomicInteger waiting_to_connect;

	HashMap<Integer, Long> reqs = new HashMap<Integer, Long>();
	ArrayList<String> times = new ArrayList<String>();
	LinkedBlockingQueue<Integer> requestQueue = new LinkedBlockingQueue<Integer>();
	String tablename;
	int NUM_COLS = 1;
	int num_rows;
	int COL_VALUE_LENGTH = 1000;
	String outdir;
	int start_version;
	int start_row;
	String prefix;
	int max_startup_delay_in_seconds = 5;
	int rateKbps;
	int object_size;
	int total_data_bytes = 0;
	int write_delay;
	String node_outfile_id;
	int client;
	int read_startup_delay_minutes;
	int write_startup_delay_minutes;

	public WriterThread(String[] args, AtomicInteger waiting_to_connect,
			String outdir_, String prefix_, String node_outfile_id,
			int writerCount) {
		tablename = args[1];
		num_rows = Integer.parseInt(args[2]);
		outdir = outdir_;
		start_version = Integer.parseInt(args[4]);
		start_row = Integer.parseInt(args[3]);
		prefix = prefix_;
		// max_startup_delay_in_seconds = Integer.parseInt(args[9]);
		// rateKbps = Integer.parseInt(args[10]);
		object_size = Integer.parseInt(args[5]);
		write_delay = Integer.parseInt(args[6]);
		read_startup_delay_minutes = Integer.parseInt(args[7]);
		write_startup_delay_minutes = Integer.parseInt(args[8]);
		client = writerCount;

		this.node_outfile_id = node_outfile_id;
		this.waiting_to_connect = waiting_to_connect;

		System.out
				.println("table=" + tablename + ", start_row=" + start_row
						+ ", num_rows=" + num_rows + ", start_version="
						+ start_version);

		try {
			properties = Utils.loadProperties("client.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void run() {

		long startTime = System.currentTimeMillis();

		// random startup delay
		try {
			LOG.debug("random startup delay");
			Thread.sleep((read_startup_delay_minutes * MINUTE)
					+ rnd.nextInt(write_startup_delay_minutes * MINUTE)); 
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		UUID randomUUID = UUID.randomUUID();
		String nodes = properties.getProperty("gateways");
		List<String> nodeArr = Arrays.asList(nodes.split("\\s*,\\s*"));
		String gw;

		// consistent hash gateway selection
		// hasher = new ConsistentHash(nodeArr);
		// gw = hasher.getNode(randomUUID.toString());

		// round robin gateway selection
		gw = nodeArr.get(client % nodeArr.size());

		LOG.debug("chosen gateway = " + gw);

		String[] str = gw.split(":");
		SimbaNetworkManager mgr = new SimbaNetworkManager(str[0],
				Integer.parseInt(str[1]), sequencer, requestQueue, client);

		try {
			LOG.debug("Give the network manager some time to setup (sleeping up to 30 seconds)...");
			 Thread.sleep((1 * MINUTE) + rnd.nextInt(2 * MINUTE));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		mgr.registerDevice(randomUUID.toString());

		waiting_to_connect.decrementAndGet();

		while (waiting_to_connect.get() != 0) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		try {
			Thread.sleep((((read_startup_delay_minutes
					+ write_startup_delay_minutes + 3) * MINUTE) - (System
					.currentTimeMillis() - startTime))
					+ rnd.nextInt(write_delay));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		int seq = 0;

		/*
		 * 
		 * /* CreateTable.Builder tbl = CreateTable.newBuilder();
		 * 
		 * tbl.setApp(KEYSPACE_NAME).setTbl(tablename);
		 * 
		 * total_data_bytes += tablename.getBytes().length; total_data_bytes +=
		 * KEYSPACE_NAME.getBytes().length;
		 * 
		 * String consistencyLevel = properties.getProperty("consistency",
		 * "CAUSAL"); consistencyLevel.toUpperCase(); if
		 * (consistencyLevel.equals("EVENTUAL")) {
		 * tbl.setConsistencyLevel(SimbaConsistency.newBuilder().setType(
		 * SimbaConsistency.Type.EVENTUAL)); } else if
		 * (consistencyLevel.equals("CAUSAL")) {
		 * tbl.setConsistencyLevel(SimbaConsistency.newBuilder().setType(
		 * SimbaConsistency.Type.CAUSAL)); } else if
		 * (consistencyLevel.equals("MONOTONIC")) {
		 * tbl.setConsistencyLevel(SimbaConsistency.newBuilder().setType(
		 * SimbaConsistency.Type.MONOTONIC)); } else if
		 * (consistencyLevel.equals("STRONG")) {
		 * tbl.setConsistencyLevel(SimbaConsistency.newBuilder().setType(
		 * SimbaConsistency.Type.STRONG)); }
		 * 
		 * total_data_bytes += consistencyLevel.getBytes().length;
		 * 
		 * for (int i = 0; i < NUM_COLS; i++) {
		 * tbl.addColumns(Column.newBuilder().setName("col" + i)
		 * .setType(Column.Type.VARCHAR).build());
		 * 
		 * total_data_bytes += new String("col" + i).getBytes().length;
		 * total_data_bytes += (Integer.SIZE / Byte.SIZE); // size of VARCHAR }
		 * 
		 * // tbl.addColumns(Column.newBuilder().setName("small") //
		 * .setType(Column.Type.OBJECT).build());
		 * 
		 * tbl.addColumns(Column.newBuilder().setName("large")
		 * .setType(Column.Type.OBJECT).build());
		 * 
		 * total_data_bytes += new String("large").getBytes().length;
		 * 
		 * mgr.createTable(tbl.build()); System.out .println("create table: " +
		 * total_data_bytes + " total bytes");
		 */

		// LOG.debug("Writing " + num_rows + " rows to table test.table");
		Long init = null;
		if (this.rateKbps > 0)
			mgr.getNetwork().startMonitoring(this.rateKbps);

		byte[] compressible = null;
		byte[] incompressible;
		int original_num_chunks = 0;

		if (object_size > 0) {
			// int original_obj_size = 1 * Preferences.MAX_FRAGMENT_SIZE;
			int original_obj_size = object_size;
			original_num_chunks = original_obj_size
					/ Preferences.MAX_FRAGMENT_SIZE;
			int bufferSize = original_obj_size;
			double compressibility_ratio = .5;

			compressible = new byte[bufferSize];
			incompressible = new byte[bufferSize];
			rnd.nextBytes(incompressible);
			for (int b = 0; b < bufferSize; b += Preferences.MAX_FRAGMENT_SIZE) {
				int incompressibleBytes = (int) (Preferences.MAX_FRAGMENT_SIZE * (1 - compressibility_ratio));
				System.arraycopy(incompressible, b, compressible, b,
						incompressibleBytes);
			}
		}
		// System.arraycopy(incompressible, 0, compressible, 0, bufferSize);

		// System.arraycopy(incompressible, 0, compressible, 0, (int)
		// (bufferSize*compressibility_ratio));

		Long beginWrite;

		for (int i = start_row; i < (start_row + num_rows); i++) {

			seq = sequencer.getSeq();

			LinkedList<ObjectFragment> fragments = new LinkedList<ObjectFragment>();
			int oid = 0;

			DataRow.Builder d = DataRow.newBuilder();

			// if (FIRST_WRITE) {
			// // byte[] data = new byte[4096];
			// // fragments.addAll(ObjectFragmenter.fragment(oid, seq, data));
			// //
			// // ObjectHeader h1 =
			// // ObjectHeader.newBuilder().setColumn("small")
			// // .setOid(oid).build();
			// // oid++;
			//
			// // fragments.addAll(ObjectFragmenter.fragment(oid, seq,
			// // compressible));
			// //
			// // ObjectHeader h2 = ObjectHeader.newBuilder().setColumn("large")
			// // .setOid(oid).build();
			// // oid++;
			//
			// // d.setId("row" +
			// // i).setRev(start_version).addObj(h1).addObj(h2);
			// // d.setId("row" + i).setRev(start_version).addObj(h2);
			// d.setId("row" + i).setRev(start_version);
			//
			// total_data_bytes += new String("row" + i).getBytes().length;
			//
			// } else {
			// replace chunk to object "large"

			if (object_size > 0) {
				ObjectFragment update = ObjectFragment.newBuilder()
						.setData(ByteString.copyFrom(compressible))
						.setEof(true).setOffset(0).setOid(oid).setTransId(seq)
						.build();
				fragments.add(update);

				total_data_bytes += compressible.length; // data size

				ObjectHeader h2 = ObjectHeader.newBuilder().setColumn("large")
						.setOid(oid).setNumChunks(original_num_chunks).build();
				oid++;

				total_data_bytes += new String("large").getBytes().length;

				d.setId("row" + i).setRev(start_version).addObj(h2);

				total_data_bytes += new String("row" + i).getBytes().length;

			} else {
				d.setId("row" + i).setRev(start_version);
			}

			// }
			// ////// d.setId("row" + i).setRev(start_version+i).addObj(h1);

			start_version++;

			// if (FIRST_WRITE) {
			for (int j = 0; j < NUM_COLS; j++) {
				String random_string = randomString(COL_VALUE_LENGTH);
				d.addData(ColumnData.newBuilder().setColumn("col" + j)
						.setValue("'" + random_string + "'").build());

				total_data_bytes += random_string.getBytes().length;
			}
			// }
			/*
			 * else { //update col0 only
			 * d.addData(ColumnData.newBuilder().setColumn("col0") .setValue("'"
			 * + randomString(COL_VALUE_LENGTH) + "'") .build()); }
			 */

			SyncHeader sd = SyncHeader.newBuilder().setApp(KEYSPACE_NAME)
					.setTbl(tablename).addDirtyRows(d.build()).setTransId(seq)
					.build();

			SyncRequest sr = SyncRequest.newBuilder().setData(sd).build();

			ClientMessage sync = ClientMessage.newBuilder()
					.setType(ClientMessage.Type.SYNC_REQUEST)
					.setSyncRequest(sr).setToken(randomUUID.toString())
					.setSeq(seq).build();

			sequencer.addPendingSeq(seq, sync);

			if (RECORD_TIME) {
				beginWrite = System.nanoTime();
				reqs.put(seq, beginWrite);
			}

			LOG.info("Sending sync message for row " + i);
			// System.out.println("send message: " + total_data_bytes
			// + " total bytes");

			mgr.sendMessage(sync);

			for (ObjectFragment f : fragments) {
				int fragSeq = sequencer.getSeq();
				ClientMessage frag = ClientMessage.newBuilder()
						.setType(ClientMessage.Type.OBJECT_FRAGMENT)
						.setObjectFragment(f).setSeq(fragSeq)
						.setToken(randomUUID.toString()).build();
				// System.out.println("Sending " + fragSeq + ", fragment " +
				// f.getOid() + " transid=" + f.getTransId());
				// try {
				// Thread.sleep(500);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				mgr.sendMessage(frag);
			}

			if (init == null) {
				init = System.nanoTime();
			}

			int r = -1;
			try {
				// LOG.debug("client"+client+": calling take()");
				r = requestQueue.take();
				// LOG.debug("client"+client+": take() returned");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (RECORD_TIME) {
				// ClientMessage m = SeqNumManager.getPendingMsg(r);
				// SeqNumManager.removePendingSeq(r);
				Long start = reqs.get(r);
				// 2 values:
				// 1: time since experiment start
				// 2: elapsed time of request
				times.add((System.nanoTime() - (double) init) / 1000000000
						+ " " + (System.nanoTime() - (double) start) / 1000000
						+ " " + r);
			}
			if (this.rateKbps > 0)
				mgr.getNetwork().sleepIfNeeded();

			if (write_delay > 0 && (i + 1 < (start_row + num_rows))) {
				try {
					long elapsed = (System.nanoTime() - beginWrite) / 1000000;
					// System.out.println("elapsed: " + elapsed);

					long sleeptime = write_delay - elapsed;
					if (sleeptime > 0) {
						// System.out.println("sleep: " + sleeptime);
						Thread.sleep(sleeptime);
					}
					// Thread.sleep(write_delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}

		if (this.rateKbps > 0) {
			System.out.println("Achieved upstream rate="
					+ mgr.getNetwork().getCurrentUpRate() + " KBps");
		}

		// LOG.debug("DUMPING LOGS...");

		if (RECORD_TIME) {

			File file = new File(outdir);
			// if the directory does not exist, create it
			if (!file.exists()) {
				System.out.println("creating directory: " + outdir);
				boolean result = file.mkdirs();

				if (result) {
					System.out.println(outdir + " created");
				}
			}

			PrintWriter out = null;
			try {
				out = new PrintWriter(new FileWriter(outdir + "/" + prefix
						+ "." + node_outfile_id + "." + client + ".writer.log"));
			} catch (IOException e1) {
				e1.printStackTrace();
			}

			for (String s : times) {
				// System.out.println(s);
				out.println(s);
			}

			out.close();
		}

		return;
	}
}
