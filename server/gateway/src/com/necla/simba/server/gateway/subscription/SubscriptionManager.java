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
package com.necla.simba.server.gateway.subscription;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.necla.simba.protocol.Common.Column;
import com.necla.simba.protocol.Common.SubscribeResponse;
import com.necla.simba.protocol.Common.SubscribeTable;
import com.necla.simba.protocol.Common.UnsubscribeTable;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.SeqNumManager;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.client.auth.InvalidTokenException;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;

public class SubscriptionManager {

	private ConcurrentHashMap<String, List<Column>> tableSchemas;

	// keeps the updated version for all currently subscribed tables
	private ConcurrentHashMap<String, Integer> tableVersions;

	// table --> client(s) mapping
	private Multimap<String, ClientState> subscribers;

	private static final Logger LOG = LoggerFactory
			.getLogger(SubscriptionManager.class);
	private BackendConnector backend;
	private ClientAuthenticationManager cam;

	public SubscriptionManager(BackendConnector client) {
		this.backend = client;
		tableSchemas = new ConcurrentHashMap<String, List<Column>>();
		tableVersions = new ConcurrentHashMap<String, Integer>();
		subscribers = Multimaps.synchronizedSetMultimap(HashMultimap
				.<String, ClientState> create());
	}

	// TODO: fix this dependency
	public void setClientAuthenticationManager(ClientAuthenticationManager cam) {
		this.cam = cam;
	}

	public void addSchema(String tablename, SubscribeResponse sr, int seq) {

		// NOTE: this is allowed occur multiple times because of the race
		// condition between multiple clients attempting to subscribe to the
		// same table. since the client relies on the sequence number of their
		// subscribe message to match the schema to the table subscribe message,
		// we service all subscribe requests until the gateway has received the
		// schema.

		// add schema if it doesnt exist for this table
		if (!tableSchemas.containsKey(tablename)) {
			// insert schema into table schema map
			tableSchemas.put(tablename, sr.getColumnsList());
		}

		// send schema to client
		SimbaMessage saved = SeqNumManager.getPendingMsg(seq);

		ClientState client = null;
		try {
			client = cam.getClientState(saved.getToken());
		} catch (InvalidTokenException e) {
			e.printStackTrace();
		}

		LOG.debug("Sending SUB_RESPONSE to client "
				+ client.getSocket());

		ClientMessage msg = ClientMessage.newBuilder()
				.setType(ClientMessage.Type.SUB_RESPONSE)
				.setSubscribeResponse(sr).setSeq(seq)
				.setToken(client.getToken()).build();
		try {
			client.sendMessage(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		SeqNumManager.removePendingSeq(seq);
	}

	public void setVersion(String tablename, int version) {
		// also add initial table version to table version map
		if (!tableVersions.containsKey(tablename)) {
			tableVersions.put(tablename, version);
		}
	}

	/**
	 * This function may be called to update the subscription for a given
	 * client too, so it should make sure that the state is updated, instead
	 * of just being appended
	 * @param client
	 * @param app
	 * @param table
	 * @param version
	 * @param clientSeq
	 * @param token
	 */
	public void subscribe(ClientState client, String app, String table,
			Integer version, Integer clientSeq, String token) {
		// If first subscription to this table, send subscribe request
		String tablename = app + "." + table;

		if (!subscribers.containsKey(tablename)
				|| !tableSchemas.containsKey(tablename)) {
			// build subscribe message
			SubscribeTable sub = SubscribeTable.newBuilder().setApp(app)
					.setTbl(table).build();

			int seq;
			if (clientSeq == null) {
				seq = SeqNumManager.getSeq();
			} else {
				seq = clientSeq;
			}

			SimbaMessage msg = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.SUB_TBL).setSubscribeTable(sub)
					.setSeq(seq).setToken(token).build();

			SeqNumManager.addPendingSeq(seq, msg);

			try {
				// send message
				backend.sendTo(tablename, msg);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// Add subscription to map (no-op if client is already there)
		subscribers.put(tablename, client);
		LOG.info("subscribe: " + tablename + " (" + client.getDeviceId() + ")");

		// check if schema is known to gateway
		if (tableSchemas.containsKey(tablename) && clientSeq != null) {
			// if clientSeq == null, this was a restore subscription request
			// and the client should already know the schema
			SubscribeResponse sr = SubscribeResponse.newBuilder()
					.addAllColumns(tableSchemas.get(tablename)).build();

			ClientMessage msg = ClientMessage.newBuilder()
					.setType(ClientMessage.Type.SUB_RESPONSE)
					.setSubscribeResponse(sr).setSeq(clientSeq)
					.setToken(client.getToken()).build();

			// send schema to client
			try {
				client.sendMessage(msg);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// check if client needs to be notified of newer version
		if (tableVersions.containsKey(tablename)) {
			int currentVersion = tableVersions.get(tablename);

			if (currentVersion > version) {
				// if table has received any updates, send notification
				client.processNotification(tablename);
			}
		}

	}

	public void unsubscribe(ClientState client, String app, String table) {
		// Remove subscription from map
		String tablename = app + "." + table;

		subscribers.remove(tablename, client);
		LOG.info("unsubscribe: " + tablename + " (" + client.getDeviceId()
				+ ")");

		// Send unsubscribe if no record for tableId
		if (!subscribers.containsKey(table)) {
			// build unsubscribe message
			UnsubscribeTable unsub = UnsubscribeTable.newBuilder().setApp(app)
					.setTbl(table).build();

			SimbaMessage msg = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.UNSUB_TBL)
					.setUnsubscribeTable(unsub).setSeq(SeqNumManager.getSeq())
					.build();

			try {
				// send message immediately
			
				backend.sendTo(tablename, msg);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public Collection<ClientState> getSubscribers(String tableId) {
		return subscribers.get(tableId);
	}

	public synchronized void processNotification(String tableId, Integer version) {
		if (tableVersions.containsKey(tableId)) {
			int currentVersion = tableVersions.get(tableId);

			// Version numbers should be monotonically increasing.
			// Since all updates for a table come from the same simbaserver,
			// this should be guaranteed.
			if (version == currentVersion + 1) {
				processNotificationInternal(tableId, version);
			} else {
				LOG.error("New version for table '" + tableId
						+ "' is invalid (" + version + " != " + currentVersion
						+ " + 1)");
			}

		} else {
			processNotificationInternal(tableId, version);
		}
	}

	private void processNotificationInternal(String tableId, int version) {
		// cache this update
		tableVersions.put(tableId, version);

		// push notification to all subscribed clients
		Collection<ClientState> clients = subscribers.get(tableId);
		synchronized (subscribers) {
			if (clients != null) {
				for (ClientState client : clients) {
					client.processNotification(tableId);
				}
			}
		}
	}

	public Integer getTableVersion(String table) {
		if (!tableVersions.containsKey(table)) {
			LOG.error("No subscription for this table! Why is client performing NotificationPull?");
			return -1;
		}
		return tableVersions.get(table);
	}
	
	public void dump(){
		StringBuilder sb = new StringBuilder();
		sb.append("Schemas: " + tableSchemas.size() + "\n");
		sb.append("Versions: " + tableVersions.size() + "\n");
		sb.append("Subscribers: " + subscribers.size() + "\n");
		System.out.println(sb.toString());
	}

}
