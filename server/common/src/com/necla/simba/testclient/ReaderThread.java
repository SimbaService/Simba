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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.client.SimbaNetworkManager;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.NotificationPull;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.SubscribeTable;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.util.ConsistentHash;
import com.necla.simba.util.Utils;

public class ReaderThread implements Runnable {

	private static final int MINUTE = 60 * 1000;
	private static final String keyspace = "test_3replicas";

	private static final Logger LOG = LoggerFactory
			.getLogger(ReaderThread.class);

	protected static Properties properties;
	protected static ConsistentHash hasher;
	Random rnd = new Random();

	private SeqNumManager sequencer = new SeqNumManager();

	String table;
	String outdir;
	String prefix;
	String client;
	int start_ver;
	int period; // in milliseconds
	int DT; // in milliseconds
	int num_rows;

	int startup_delay_minutes;
	
	LinkedBlockingQueue<ClientMessage> requestQueue = new LinkedBlockingQueue<ClientMessage>();

	int seq;
	UUID randomUUID;
	int curver = -1;
	boolean pull_on_sub_response;

	String gw;
	SimbaNetworkManager mgr;

	BufferedWriter out;
	// PrintWriter out;

	AtomicInteger waiting_to_connect;
	String node_outfile_id;

	int notifyCounter = 0;

	public ReaderThread(String args[], AtomicInteger waiting_to_connect,
			String outdir_, String prefix_, int num, String node_outfile_id) {
		LOG.debug("Starting Client...");
		table = args[1];
		outdir = outdir_;
		prefix = prefix_;
		client = Integer.toString(num);
		curver = Integer.parseInt(args[4]);
		// curver = 0;
		period = Integer.parseInt(args[2]);
		DT = Integer.parseInt(args[3]);
		pull_on_sub_response = false;
		num_rows = Integer.parseInt(args[5]);
		startup_delay_minutes = Integer.parseInt(args[6]);

		this.node_outfile_id = node_outfile_id;
		this.waiting_to_connect = waiting_to_connect;

		try {
			properties = Utils.loadProperties("client.properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		randomUUID = UUID.randomUUID();

		File file = new File(outdir);
		// if the directory does not exist, create it
		if (!file.exists()) {
			System.out.println("creating directory: " + outdir);
			boolean result = file.mkdirs();

			if (result) {
				System.out.println(outdir + " created");
			}
		}

		out = null;
		try {
			out = new BufferedWriter(new FileWriter(outdir + "/" + prefix + "."
					+ node_outfile_id + "." + client + ".reader.log"));
		} catch (IOException e1) { 
			e1.printStackTrace();
		}
	}

	public void run() {

		// random startup delay

		try {
			LOG.debug("random startup delay");
			 Thread.sleep(rnd.nextInt(startup_delay_minutes * MINUTE));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String nodes = properties.getProperty("gateways");
		List<String> nodeArr = Arrays.asList(nodes.split("\\s*,\\s*"));

		// consistent hash gateway selection
		// hasher = new ConsistentHash(nodeArr);
		// gw = hasher.getNode(randomUUID.toString());

		// round robin gateway selection
		gw = nodeArr.get(Integer.parseInt(client) % nodeArr.size());

		LOG.debug("chosen gateway = " + gw);

		String[] str = gw.split(":");

		mgr = new SimbaNetworkManager(str[0], Integer.parseInt(str[1]),
				sequencer, requestQueue, Integer.parseInt(client));

		System.out.println("got connection");

		try {
			LOG.debug("Give the network manager some time to setup (sleeping up to 5 seconds)...");
			Thread.sleep(1000 + rnd.nextInt(4000));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// REGISTER DEVICE

		mgr.registerDevice(randomUUID.toString());

		// SUBSCRIBE

		seq = sequencer.getSeq();

		// int[] periods = { 5000, 10000, 15000, 20000, 25000, 30000 };
		// // DT = 0;//rnd.nextInt(5) * 1000;

		// int[] periods = { 500, 1000, 1500, 2000, 2500, 3000 };
		// int choice = rnd.nextInt(periods.length);
		// period = periods[choice];

		SubscribeTable st = SubscribeTable.newBuilder().setApp(keyspace)
				.setTbl(table).setRev(curver).setPeriod(period)
				.setDelayTolerance(DT).build();

		ClientMessage m2 = ClientMessage.newBuilder().setSubscribeTable(st)
				.setType(ClientMessage.Type.SUB_TBL).setSeq(seq)
				.setToken(randomUUID.toString()).build();

		sequencer.addPendingSeq(seq, m2);
		mgr.sendMessage(m2);

		ClientMessage mmsg = null;

		try {
			mmsg = requestQueue.take();
			// System.out.println("top level: q.take() returned");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		final int t = mmsg.getType().getNumber();

		if (t == ClientMessage.Type.SUB_RESPONSE_VALUE) {
			LOG.debug("client" + client + ": SUB_RESPONSE");
		} else {
			LOG.error("client" + client + ": NOT SUB RESPONSE! ABORT!");
			System.exit(1);
		}

		waiting_to_connect.decrementAndGet();

		int c = 0;
		while (waiting_to_connect.get() != 0) {
			try {
				if (c % 120 == 0) {
					System.out.println("waiting on others...");
				}
				Thread.sleep(1 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			c++;
		}

		System.out.println("ALL CLIENTS CONNECTED!");

		Long init = System.nanoTime();

		HashMap<Integer, Integer> transObjMap = new HashMap<Integer, Integer>();
		HashMap<Integer, Long> latMap = new HashMap<Integer, Long>();
		// keep track of # of object fragments for a given trans id

		int expected_objects = 0;

		while (true) {

			if (0 == num_rows && expected_objects == 0) {
				break;
			}

			try {
				mmsg = requestQueue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			final int type = mmsg.getType().getNumber();

			if (type == ClientMessage.Type.BITMAP_NOTIFY_VALUE) {
				notifyCounter++;

				// PULL N ROWS

				seq = sequencer.getSeq();

				System.out.println("client=" + client + ": Sending NOTIFICATION_PULL, from_version="
						+ curver);

				NotificationPull np = NotificationPull.newBuilder()
						.setApp(keyspace).setTbl(table).setFromVersion(curver)
						.setToVersion(curver + 1).build();

				ClientMessage notificationpull = ClientMessage.newBuilder()
						.setType(ClientMessage.Type.NOTIFICATION_PULL)
						.setNotificationPull(np)
						.setToken(randomUUID.toString()).setSeq(seq).build();

				sequencer.addPendingSeq(seq, notificationpull);

				LOG.debug(notificationpull.getType().toString());

				mgr.sendMessage(notificationpull);

				latMap.put(seq, System.nanoTime());

			} else if (type == ClientMessage.Type.PULL_DATA_VALUE) {

				SyncHeader s = mmsg.getPullData().getData();

				int total_objs = 0;

				if (s.getDirtyRowsCount() > 0) {
					for (DataRow row : s.getDirtyRowsList()) {

						if (row.getObjCount() > 0) {
							total_objs += row.getObjCount();
							expected_objects += total_objs;
						}

						if (row.getRev() > curver) {
							curver = row.getRev();
						}

						num_rows--;
					}
				}

				if (total_objs > 0) {
					transObjMap.put(s.getTransId(), total_objs);
				} else {
					long current_time = System.nanoTime();
					long start = latMap.get(s.getTransId());
					try {
						// 4 values:
						// 1: time since experiment start
						// 2: time of request intiation relative to start
						// 3: elapsed time of request
						// 4: transaction id
						out.write((current_time - (double) init) / 1000000000
								+ " " + (start - (double) init) / 1000000 + " "
								+ (current_time - (double) start) / 1000000
								+ " " + s.getTransId() + "\n");
						out.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

			} else if (type == ClientMessage.Type.OBJECT_FRAGMENT_VALUE) {
				ObjectFragment o = mmsg.getObjectFragment();
				int num_objs = -1;
				if (o.hasEof()) {
					if (o.getEof() == true) {

					}
					if (transObjMap.containsKey(o.getTransId())) {
						num_objs = transObjMap.get(o.getTransId());
					}

					num_objs--;
					expected_objects--;

					if (num_objs == 0) {
						transObjMap.remove(o.getTransId());
						Long start = latMap.get(o.getTransId());
						// write time
						long current_time = System.nanoTime();
						try {
							out.write((current_time - (double) init)
									/ 1000000000 + " "
									+ (start - (double) init) / 1000000 + " "
									+ (current_time - (double) start) / 1000000
									+ " " + o.getTransId() + "\n");
							out.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}
					} else {
						transObjMap.put(o.getTransId(), num_objs);
					}

					if (num_objs == -1) {
						System.out.println("num_objs == -1; ERROR");
					}

				}
			} else {
				LOG.error("unexpected message, "
						+ ClientMessage.Type.valueOf(type).name());
			}
		}
	}
}
