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
package com.necla.simba.server.simbastore;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.simbastore.cassandra.CassandraHandler;
import com.necla.simba.server.simbastore.server.GatewayManager;
import com.necla.simba.server.simbastore.server.SimbaStoreServer;
import com.necla.simba.server.simbastore.server.SubscriptionManager;
import com.necla.simba.server.simbastore.server.data.ServerDataEvent;
import com.necla.simba.server.simbastore.server.thread.SimbaStoreWorker;
import com.necla.simba.server.simbastore.stats.BackendStats;
import com.necla.simba.server.simbastore.stats.IOStats;
import com.necla.simba.server.simbastore.swift.SwiftHandler;
import com.necla.simba.server.simbastore.table.SimbaTable;
import com.necla.simba.server.simbastore.table.SyncTransaction;
import com.necla.simba.util.ConsistentHash;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	private Properties properties;
	private SimbaStoreServer server;
	private Options options = new Options();
	private LinkedBlockingQueue[] queues;
	private Thread[] workerThreads;
	private ConsistentHash hasher;
	private GatewayManager gm;
	private static SwiftHandler objectStore;
	private static CassandraHandler tableStore;
	private static ConcurrentHashMap<String, SimbaTable> tables;
	private static SubscriptionManager subscriptionManager;
	private static ConcurrentHashMap<Integer, SyncTransaction> transactions;

	public Main(String[] args) {
		options.addOption("h", "help", false, "print help message");
		options.addOption(OptionBuilder
				.hasArgs(1)
				.withLongOpt("properties")
				.withDescription(
						"SimbaStore properties file, this overrides default properties")
				.withType(String.class).create("p"));
		CommandLineParser parser = new org.apache.commons.cli.BasicParser();
		try {
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("help"))
				usage();
			properties = new Properties();
			properties.load(Main.class
					.getResourceAsStream("/simbastore.properties"));
			if (line.hasOption("properties"))
				properties.load(new FileInputStream(line
						.getOptionValue("properties")));
		} catch (ParseException e) {
			usage();
		} catch (IOException e) {
			LOG.error("Could not load properties: " + e.getMessage());
			System.exit(1);
		}

		IOStats.start();
		BackendStats.start();
	}

	public void serve() {
		try {
			tables = new ConcurrentHashMap<String, SimbaTable>();
			objectStore = new SwiftHandler(properties);
			tableStore = new CassandraHandler(properties);
			transactions = new ConcurrentHashMap<Integer, SyncTransaction>();

			int threads = Integer.parseInt(properties
					.getProperty("thread.count"));

			queues = new LinkedBlockingQueue[threads];
			workerThreads = new Thread[threads];

			hasher = new ConsistentHash();

			gm = new GatewayManager(queues, properties);

			server = new SimbaStoreServer(InetAddress.getLocalHost(),
					Integer.parseInt(properties.getProperty("port")), queues,
					hasher, gm);

			subscriptionManager = new SubscriptionManager(server);

			for (int i = 0; i < threads; i++) {
				queues[i] = new LinkedBlockingQueue<ServerDataEvent>();

				SimbaStoreWorker worker = new SimbaStoreWorker(properties,
						queues[i], objectStore, tableStore, tables,
						subscriptionManager, transactions, gm);

				worker.setSimbaServer(server);
				workerThreads[i] = new Thread(worker, Integer.toString(i));
				workerThreads[i].start();
			}

			new Thread(server, "server").start();

			gm.setServer(server);
			gm.start();

			LOG.debug("SimbaServer start-up complete.");

		} catch (IOException e) {
			LOG.error("Could not start SimbaServer: " + e.getMessage());
			System.exit(1);
		}

	}

	private void usage() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("options", options);
		System.exit(1);
	}

	public static void main(String[] args) {
		Main main = new Main(args);
		main.serve();
	}
}
