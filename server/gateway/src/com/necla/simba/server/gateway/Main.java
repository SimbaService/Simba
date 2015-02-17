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
package com.necla.simba.server.gateway;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.client.SeqNumManager;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.client.notification.NotificationManager;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.server.frontend.FrontendFrameEncoder;
import com.necla.simba.server.gateway.server.frontend.FrontendServer;
import com.necla.simba.server.gateway.stats.StatsDumper;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.util.StatsCollector;

/**
 * @author dperkins@nec-labs.com
 * @created May 30, 2013 1:27:03 PM
 */

public class Main {

	/**
	 * @param args
	 */
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	public static Properties properties;

	private BackendConnector backend;
	private StatsCollector stats;
	private SubscriptionManager subscriptionManager;
	private ClientAuthenticationManager cam;
	private Options options = new Options();

	public static ExecutorService compressionService = Executors
			.newFixedThreadPool(32);

	private void usage() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("options", options);
		System.exit(1);
	}

	public Main(String[] args) {
		options.addOption("h", "help", false, "print help message");
		options.addOption(OptionBuilder
				.hasArgs(1)
				.withLongOpt("properties")
				.withDescription(
						"Gateway properties file, this overrides default properties")
				.withType(String.class).create("p"));
		options.addOption(OptionBuilder.hasArgs(1).withLongOpt("port")
				.withDescription("client communication port")
				.withType(Number.class).create("o"));
		CommandLineParser parser = new org.apache.commons.cli.BasicParser();
		try {
			properties = new Properties();
			properties.put("port", 9000);

			CommandLine line = parser.parse(options, args);
			if (line.hasOption("help"))
				usage();
			properties.load(Main.class
					.getResourceAsStream("/gateway.properties"));
			if (line.hasOption("properties"))
				properties.load(new FileInputStream(line
						.getOptionValue("properties")));
			if (line.hasOption("port"))
				properties.put("port",
						Integer.parseInt(line.getOptionValue("port")));
			// dump all notification info
		} catch (ParseException e) {
			usage();
		} catch (IOException e) {
			LOG.error("Could not load properties: " + e.getMessage());
			System.exit(1);
		}
	}

	public void serve() {
		try {
			
			stats = new StatsCollector();
            stats.start("gateway.out");
			boolean frontendCompression = Boolean.parseBoolean(properties.getProperty("frontend.compression", "true"));
			FrontendFrameEncoder.DOCOMPRESS = frontendCompression;

			
			String nodes = properties.getProperty("simbastores");
			List<String> nodeArr = Arrays.asList(nodes.split("\\s*,\\s*"));
            backend = new BackendConnector(stats);
			subscriptionManager = new SubscriptionManager(backend);
			SeqNumManager sequencer = new SeqNumManager();
			final NotificationManager nom = new NotificationManager(sequencer);
			cam = new ClientAuthenticationManager(subscriptionManager, nom, backend, sequencer);
			subscriptionManager.setClientAuthenticationManager(cam);

			// dump all notification info
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(new TimerTask() {
				public void run() {
					// LOG.debug("Flushing print buffer...");
					nom.dump();
					subscriptionManager.dump();
				}
			}, 0, 30000);
			StatsDumper dumper = new StatsDumper();
			dumper.start();
			
			
			
			backend.connect(properties, nodeArr, subscriptionManager, cam);
			FrontendServer frontend = new FrontendServer(properties, backend, stats, subscriptionManager, cam);

			
		
			LOG.debug("Gateway start-up complete.");

		} catch (IOException e) {
			LOG.error("Could not start server " + e.getMessage());
			System.exit(1);
		} catch (URISyntaxException e) {
			LOG.error("Invalid host specification: " + e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			LOG.error("Could not start server " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// BasicConfigurator.configure();
		Main main = new Main(args);
		main.serve();

	}
}
