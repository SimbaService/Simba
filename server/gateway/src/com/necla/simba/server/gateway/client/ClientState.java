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
package com.necla.simba.server.gateway.client;

import io.netty.channel.Channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Client.BitmapNotify;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.protocol.Client.Notify;
import com.necla.simba.protocol.Server.ClientSubscription;
import com.necla.simba.protocol.Server.RequestClientSubscriptions;
import com.necla.simba.protocol.Server.RestoreClientSubscriptions;
import com.necla.simba.protocol.Server.SaveClientSubscription;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.Main;
import com.necla.simba.server.gateway.client.notification.NotificationManager;
import com.necla.simba.server.gateway.client.notification.NotificationTask;
import com.necla.simba.server.gateway.client.sync.SyncScheduler;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.subscription.Subscription;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.util.ConsistentHash;

public class ClientState {
	private static final Logger LOG = LoggerFactory
			.getLogger(ClientState.class);

	private String deviceId;
	private String token;
	private Channel pc;
	private BitSet bitmap;
	public boolean isConnected;
	Long lastConnect;
	public ArrayList<Subscription> subscriptions;
	private SyncScheduler scheduler;
	private SubscriptionManager subscriptionManager;
	private BackendConnector client;
	private HashMap<Integer, String> transactions = new HashMap<Integer, String>();
	private final ReadWriteLock subscriptionReadWriteLock = new ReentrantReadWriteLock();
	public final Lock subscriptionRead = subscriptionReadWriteLock.readLock();
	final Lock subscriptionWrite = subscriptionReadWriteLock.writeLock();
	private final ReadWriteLock bitmapReadWriteLock = new ReentrantReadWriteLock();
	public final Lock bitmapRead = bitmapReadWriteLock.readLock();
	public final Lock bitmapWrite = bitmapReadWriteLock.writeLock();
	private ConsistentHash hasher;
	private NotificationManager nom;
	private SeqNumManager sequencer;

	private static boolean BITMAP_NOTIFICATION = Boolean
			.parseBoolean(Main.properties.getProperty("bitmap.notification"));
	private static int NO_DELAY = 0;

	public ClientState(String id, Channel pc, String token,
			SubscriptionManager subscriptionManager, NotificationManager nom,
			BackendConnector client, SeqNumManager seq) {
		// public ClientState(String id, PacketChannel pc, String token, boolean
		// restore){
		bitmap = new BitSet();
		this.deviceId = id;
		this.token = token;
		this.pc = pc;
		this.nom = nom;
		this.subscriptions = new ArrayList<Subscription>();
		this.isConnected = true;

		this.subscriptionManager = subscriptionManager;
		this.client = client;
		this.scheduler = new SyncScheduler(this);
		this.sequencer = seq;

		hasher = new ConsistentHash();
		// if(restore){
		// requestClientSubscriptions();
		// }
	}

	public void addTransaction(int transId, String table) {
		transactions.put(transId, table);
	}

	public String getTransaction(int transId) {
		return transactions.get(transId);
	}

	public SyncScheduler getSyncScheduler() {
		if (this.scheduler == null)
			this.scheduler = new SyncScheduler(this);
		return this.scheduler;
	}

	public void processNotification(String table) {

		LOG.debug("Setting bit for table: " + table + "("
				+ subscriptions.indexOf(new Subscription(table, 0, 0)) + ")");

		// LOG.debug("Current # of subscriptions:" + subscriptions.size());
		// LOG.debug("Subscription list:");
		// for(String n : subscriptions){
		// LOG.debug(n);
		// }
		// LOG.debug("Index of " + table + ": " );

		Subscription sub;
		int index;

		subscriptionRead.lock();
		try {
			// find index of matching subscription
			index = subscriptions.indexOf(new Subscription(table, 0, 0));

			// get subscription
			sub = subscriptions.get(index);
		} finally {
			subscriptionRead.unlock();
		}

		// check for zero period special case
		// table is using STRONG consistency
		if (sub.period == 0) {

			// send notification immediately
			if (this.isConnected) {

				// create new temporary bitmap with the bit set for this table
				BitSet notification = new BitSet();
				subscriptionRead.lock();
				try {
					notification.set(subscriptions.indexOf(new Subscription(
							table, 0, 0)));
				} finally {
					subscriptionRead.unlock();
				}
				
				// build message
				ClientMessage.Builder m = ClientMessage.newBuilder();
				if (BITMAP_NOTIFICATION) {
					BitmapNotify n = BitmapNotify
							.newBuilder()
							.setBitmap(
									ByteString.copyFrom(NotificationTask
											.bitsToByteArray(notification)))
							.build();

					m.setType(ClientMessage.Type.BITMAP_NOTIFY)
							.setBitmapNotify(n).setSeq(this.sequencer.getSeq())
							.build();

					System.out.println("Queueing BITMAP_NOTIFY for "
							+ this.deviceId);

				} else {
					Notify n = Notify.newBuilder().build();

					m.setType(ClientMessage.Type.NOTIFY).setNotify(n)
							.setSeq(this.sequencer.getSeq()).build();

					System.out.println("Queueing SINGLE BIT NOTIFY for "
							+ this.deviceId);
				}

				// schedule message
				this.scheduler.schedule(m.build(), NO_DELAY);

			} else {
				LOG.debug("Client is not connected");
			}
		} else {
			// normal case

			bitmapWrite.lock();
			try {
				// set bit for this table subscription in bitmap
				bitmap.set(index);
			} finally {
				bitmapWrite.unlock();
			}
		}
	}

	public void addSubscription(String app, String table, int period, int dt,
			boolean restore, int version, Integer seq) {
		boolean isNewSubscription = true;
		// add to subscription list
		String tablename = app + "." + table;
		LOG.debug("Add subscription for table=" + tablename + " at period "
				+ period);
		Subscription sub = new Subscription(tablename, period, dt);

		subscriptionWrite.lock();
		try {
			for (int i = 0; i < subscriptions.size(); i++) {
				if (sub.equals(subscriptions.get(i))) {
					isNewSubscription = false;
					LOG.debug("found existing subscription");
					subscriptions.set(i, sub);
				}
			}
			if (isNewSubscription) {
				subscriptions.add(sub);
			}
		} finally {
			subscriptionWrite.unlock();
		}

		LOG.debug("Add subscription for SEQ=" + seq);

		if (period != 0) {
			if (!isNewSubscription) {
				// remove the existing timer task before adding the new one
				nom.removeTask(tablename, this);
			}

			// add subscription to bucket for this timer.
			nom.addTask(sub, this);
		}

		// this will overwrite an existing subscription for a given <client,
		// table> combo
		subscriptionManager.subscribe(this, app, table, version, seq, token);

		// save new subscription to cassandra
		// only if not a restore operation
		if (!restore) {
			saveClientSubscription(sub);
		}

		// send ACK to client
		// TODO: add control message for subscription addition
	}

	public void removeSubscription(String app, String table) {
		// remove subscription from table
		subscriptionWrite.lock();
		try {
			subscriptions.remove(app + "." + table);
		} finally {
			subscriptionWrite.unlock();
		}

		// Remove subscription to subscription manager
		subscriptionManager.unsubscribe(this, app, table);

		// send ACK to client
		// TODO: add control message for subscription removal
	}

	public void restoreClientState(RestoreClientSubscriptions rcs) {
		LOG.debug("Restoring subscriptions for client " + deviceId);
		for (ClientSubscription sub : rcs.getSubList()) {
			LOG.debug("Restoring subscription for " + sub.getTable() + ", p="
					+ sub.getPeriod() + ", DT=" + sub.getDt());
			String[] words = sub.getTable().split("\\.");
			addSubscription(words[0], words[1], sub.getPeriod(), sub.getDt(),
					true, -1, null);
			// TODO: which version number does client have on a restore?
		}
	}

	public void requestClientSubscriptions() {

		LOG.debug("Requesting subscriptions for client " + deviceId);
		RequestClientSubscriptions.Builder rcs = RequestClientSubscriptions
				.newBuilder();

		rcs.setClientId(deviceId);

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.REQUEST_CLIENT_SUBSCRIPTIONS)
				.setRequestClientSubscriptions(rcs.build()).build();

		try {
			client.sendTo(this.deviceId, msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void saveClientSubscription(Subscription sub) {
		// save a single subscription
		SaveClientSubscription.Builder scs = SaveClientSubscription
				.newBuilder();
		scs.setClientId(deviceId);

		ClientSubscription.Builder s = ClientSubscription.newBuilder();
		s.setTable(sub.table).setPeriod(sub.period).setDt(sub.delayTolerance)
				.build();

		scs.setSub(s);

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.SAVE_CLIENT_SUBSCRIPTION)
				.setSaveClientSubscription(scs.build()).build();

		try {
			client.sendTo(this.deviceId, msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// String server = client.hasher.getNode(this.deviceId);
		// SocketChannel socket = client.serverSockets.get(server);
		// try {
		// client.send(msg.toByteArray(), socket);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	// private void saveAllClientSubscriptions(){
	// SaveClientSubscriptions.Builder scs =
	// SaveClientSubscriptions.newBuilder();
	// scs.setClientId(deviceId);
	//
	// ClientSubscription.Builder s = ClientSubscription.newBuilder();
	// for (Subscription sub : this.subscriptions){
	// s.setTable(sub.table)
	// .setPeriod(sub.period)
	// .setDt(sub.delayTolerance)
	// .build();
	//
	// scs.addSub(s);
	// }
	//
	// SimbaMessage msg = SimbaMessage.newBuilder()
	// .setType(SimbaMessage.Type.SAVE_CLIENT_SUBSCRIPTIONS)
	// .setSaveClientSubscriptions(scs.build())
	// .build();
	//
	// String server = Main.client.hasher.getNode(this.deviceId);
	// SocketChannel socket = Main.client.serverSockets.get(server);
	// try {
	// Main.client.send(msg.toByteArray(), socket);
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }

	public void sendMessage(ClientMessage m) throws IOException {
		LOG.debug("Sending msg type=" + m.getType() + " to " + this.pc);
		// sendMessageInternal(this.pc,
		// ClientMultiMessage.newBuilder().addElementMessages(m).build());

		// StatsCollector.write(System.currentTimeMillis() + " seq " +
		// m.getSeq() + " type " + m.getType().name() +" out");

		sendMessageInternal(this.pc, ClientMultiMessage.newBuilder()
				.addMessages(m).build());
	}

	public static void sendMessage(Channel pc, ClientMessage m)
			throws IOException {
		LOG.debug("Sending msg type=" + m.getType() + " to " + pc);
		// sendMessageInternal(pc,
		// ClientMultiMessage.newBuilder().addElementMessages(m).build());
		sendMessageInternal(pc, ClientMultiMessage.newBuilder().addMessages(m)
				.build());
	}

	/*
	 * combine the two into one: provider a function that they both can call to
	 * compress
	 */

	private static void sendMessageInternal(Channel pc, ClientMultiMessage m)
			throws IOException {
		pc.writeAndFlush(m);
	}

	public void sendMultiMessage(ClientMultiMessage mm) throws IOException {
		sendMessageInternal(this.pc, mm);
	}

	public void update(String token, Channel pc) {
		this.token = token;
		this.pc = pc;
	}

	public void updatePacketChannel(Channel pc) {
		this.pc = pc;
	}

	public void updateToken(String token) {
		this.token = token;
	}

	public String getToken() {
		return this.token;
	}

	public Channel getSocket() {
		return this.pc;
	}

	public BitSet getBitmap() {
		bitmapRead.lock();
		try {
			return this.bitmap;
		} finally {
			bitmapRead.unlock();
		}
	}

	public void clearBitmap() {
		bitmapWrite.lock();
		try {
			this.bitmap.clear();
		} finally {
			bitmapWrite.unlock();
		}
	}

	public String getDeviceId() {
		return this.deviceId;
	}
}
