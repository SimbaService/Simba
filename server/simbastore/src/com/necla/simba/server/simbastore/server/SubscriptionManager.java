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
package com.necla.simba.server.simbastore.server;

import java.nio.channels.SocketChannel;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.necla.simba.protocol.Server.Notification;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.simbastore.server.SimbaStoreServer;
import com.necla.simba.server.simbastore.server.SubscriptionManager;

public class SubscriptionManager {
	private Multimap<String, SocketChannel> subscriptions;
	private static final Logger LOG = LoggerFactory
			.getLogger(SubscriptionManager.class);
	private SimbaStoreServer server;

	public SubscriptionManager(SimbaStoreServer server) {
		this.server = server;
		subscriptions = Multimaps.synchronizedSetMultimap(HashMultimap
				.<String, SocketChannel> create());
	}

	public boolean contains(String tableId) {
		return subscriptions.containsKey(tableId);
	}

	public void subscribe(String tableId, SocketChannel gateway) {
		subscriptions.put(tableId, gateway);
		System.out.println("subscribe: " + tableId + " ("
				+ gateway.socket().getInetAddress().getHostName() + ")");
	}

	public void unsubscribe(String tableId, SocketChannel gateway) {
		subscriptions.remove(tableId, gateway);
		LOG.info("unsubscribe: " + tableId + " ("
				+ gateway.socket().getInetAddress().getHostName() + ")");
	}

	public Collection<SocketChannel> getGateways(String tableId) {
		return subscriptions.get(tableId);
	}

	public void sendUpdate(String tableId, Integer version) {
		Collection<SocketChannel> gateways = subscriptions.get(tableId);
		System.out.println("gateways="+gateways);
		if (gateways != null) {
			LOG.info("sending update: (" + tableId + "," + version + ") to "
					+ gateways.size() + " gateways");

			// Build notification
			Notification n = Notification.newBuilder().setTable(tableId)
					.setVersion(version).build();

			// Build simba message
			SimbaMessage m = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.NOTIFY).setNotify(n).build();

			for (SocketChannel channel : gateways) {
				// Enqueue notification
				server.send(channel, m.toByteString().toByteArray());
			}
		}
	}
}
