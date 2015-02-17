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
package com.necla.simba.server.simbastore.server.thread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Common.AbortTransaction;
import com.necla.simba.protocol.Common.Column;
import com.necla.simba.protocol.Common.ColumnData;
import com.necla.simba.protocol.Common.CreateTable;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.DropTable;
import com.necla.simba.protocol.Common.NotificationPull;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.PullData;
import com.necla.simba.protocol.Common.SubscribeResponse;
import com.necla.simba.protocol.Common.SubscribeTable;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncRequest;
import com.necla.simba.protocol.Common.SyncResponse;
import com.necla.simba.protocol.Common.TornRowRequest;
import com.necla.simba.protocol.Common.TornRowResponse;
import com.necla.simba.protocol.Common.UnsubscribeTable;
import com.necla.simba.protocol.Server.ClientSubscription;
import com.necla.simba.protocol.Server.GetRowsRequest;
import com.necla.simba.protocol.Server.GetRowsResponse;
import com.necla.simba.protocol.Server.OperationResponse;
import com.necla.simba.protocol.Server.Ping;
import com.necla.simba.protocol.Server.RequestClientSubscriptions;
import com.necla.simba.protocol.Server.RestoreClientSubscriptions;
import com.necla.simba.protocol.Server.SaveClientSubscription;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.protocol.Server.TableVersion;
import com.necla.simba.server.simbastore.server.thread.SimbaStoreWorker;
import com.necla.simba.server.simbastore.cache.ChangeSet;
import com.necla.simba.server.simbastore.cache.ObjectChanges;
import com.necla.simba.server.simbastore.cassandra.CassandraHandler;
import com.necla.simba.server.simbastore.server.GatewayManager;
import com.necla.simba.server.simbastore.server.SimbaStoreServer;
import com.necla.simba.server.simbastore.server.SubscriptionManager;
import com.necla.simba.server.simbastore.server.data.ServerDataEvent;
import com.necla.simba.server.simbastore.stats.BackendStats;
import com.necla.simba.server.simbastore.swift.SwiftHandler;
import com.necla.simba.server.simbastore.table.SimbaTable;
import com.necla.simba.server.simbastore.table.SyncTransaction;
import com.necla.simba.server.simbastore.util.Utils;
import com.necla.simba.util.StatsCollector;

public class SimbaStoreWorker implements Runnable {
	private static final Logger LOG = LoggerFactory
			.getLogger(SimbaStoreWorker.class);

	private LinkedBlockingQueue<ServerDataEvent> queue;

	private SeqNumManager sequencer = new SeqNumManager();

	private SwiftHandler objectStore;

	private CassandraHandler tableStore;

	private Map<String, SimbaTable> tables;
	private SubscriptionManager subscriptionManager;
	private ConcurrentHashMap<Integer, SyncTransaction> transactions;
	private Properties properties;
	private SimbaStoreServer server;
	private GatewayManager gm;

	private static final ReadWriteLock tableReadWriteLock = new ReentrantReadWriteLock();
	private static final Lock tableRead = tableReadWriteLock.readLock();
	private static final Lock tableWrite = tableReadWriteLock.writeLock();

	public SimbaStoreWorker(Properties props,
			LinkedBlockingQueue<ServerDataEvent> queue,
			SwiftHandler objectStore, CassandraHandler tableStore,
			Map<String, SimbaTable> tables,
			SubscriptionManager subscriptionManager,
			ConcurrentHashMap<Integer, SyncTransaction> transactions,
			GatewayManager gm) {
		this.objectStore = objectStore;
		this.tableStore = tableStore;
		this.tables = tables;
		this.subscriptionManager = subscriptionManager;
		this.transactions = transactions;
		this.properties = props;
		this.queue = queue;
		this.gm = gm;
	}

	public void setSimbaServer(SimbaStoreServer server) {
		this.server = server;
	}

	public void run() {
		LOG.info("Started SimbaStoreWorker-" + Thread.currentThread().getName());

		ServerDataEvent sde = null;
		SimbaMessage msg = null;

		while (true) {
			try {
				sde = queue.take();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}

			msg = sde.msg;

			LOG.info("Received MSG: " + msg.getType());
			switch (msg.getType().getNumber()) {

			case SimbaMessage.Type.OBJECT_FRAGMENT_VALUE:
				if (msg.hasObjectFragment()) {
					handleObjectFragment(msg.getObjectFragment(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.SYNC_REQUEST_VALUE:
				if (msg.hasSyncRequest()) {
					handleNewTransaction(msg.getSyncRequest(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.PULL_DATA_VALUE:
				if (msg.hasPullData()) {
					handlePullData(msg.getPullData(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.TORN_REQUEST_VALUE:
				if (msg.hasTornRowRequest()) {
					handleTornRow(msg.getTornRowRequest(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.NOTIFY_PULL_VALUE:
				if (msg.hasNotifypull()) {
					handleNotifyPull(msg.getNotifypull(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.CREATE_TBL_VALUE:
				if (msg.hasCreateTable()) {
					handleCreateTable(msg.getCreateTable(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.DROP_TBL_VALUE:
				if (msg.hasDropTable()) {
					handleDropTable(msg.getDropTable(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.SUB_TBL_VALUE:
				if (msg.hasSubscribeTable()) {
					handleSubscribeTable(msg.getSubscribeTable(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.UNSUB_TBL_VALUE:
				if (msg.hasUnsubscribeTable()) {
					handleUnsubscribeTable(msg.getUnsubscribeTable(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.SAVE_CLIENT_SUBSCRIPTION_VALUE:
				if (msg.hasSaveClientSubscription()) {
					handleSaveClientSubscription(
							msg.getSaveClientSubscription(), msg.getToken(),
							msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.REQUEST_CLIENT_SUBSCRIPTIONS_VALUE:
				if (msg.hasRequestClientSubscriptions()) {
					handleRequestClientSubscriptions(
							msg.getRequestClientSubscriptions(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.PING_VALUE:
				if (msg.hasPing()) {
					handlePing(sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.ABORT_TRANSACTION_VALUE:
				if (msg.hasAbortTransaction()) {
					handleAbortTransaction(msg.getAbortTransaction(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;

			case SimbaMessage.Type.GET_ROWS_REQUEST_VALUE:
				if (msg.hasGetRowsRequest()) {
					handleGetRowsRequest(msg.getGetRowsRequest(),
							msg.getToken(), msg.getSeq(), sde.socket);
				} else {
					LOG.error(msg.getType()
							+ "message does not contain correct parameters");
					sendOperationResponse(false, msg.getType()
							+ "message does not contain correct parameters",
							msg.getToken(), msg.getSeq(), sde.socket);
				}
				break;
			}
		}
	}

	private void handleGetRowsRequest(GetRowsRequest msg, String token,
			int seq, SocketChannel socket) {
		SimbaTable table = findOrRecover(msg.getApp(), msg.getTbl());
		if (table == null) {

			// Changed this to OperationResponse since sendTableNotFound is a
			// SyncResponse and this method is not handling a SyncRequest. Maybe
			// we should make sendTableNotFond generic by using
			// OperationResponse?
			sendOperationResponse(
					false,
					"Table " + msg.getApp() + "." + msg.getTbl() + " not found",
					token, seq, socket);

			return;
		}

		int limit = -1;
		String startRowKey = null;
		if (msg.hasLimit())
			limit = msg.getLimit();
		if (msg.hasStartRowId())
			startRowKey = msg.getStartRowId();

		Iterator<Row> iter = table.getRows(startRowKey, limit);
		SyncHeader.Builder hdr = SyncHeader.newBuilder().setApp(msg.getApp())
				.setTbl(msg.getTbl()).setTransId(0);
		List<Column> schema = table.getSchema();

		try {
			while (iter.hasNext()) {
				Row r = iter.next();
				if (r.getBool("deleted"))
					continue;
				DataRow dr = Utils.rowToDataRow(r, false, 0, null, schema);
				hdr.addDirtyRows(dr);
			}

			SimbaMessage m = SimbaMessage
					.newBuilder()
					.setType(SimbaMessage.Type.GET_ROWS_RESPONSE)
					.setSeq(seq)
					.setToken(token)
					.setGetRowsResponse(
							GetRowsResponse.newBuilder().setData(hdr)).build();
			server.send(socket, m.toByteString().toByteArray());

		} catch (IOException e) {
			sendOperationResponse(false, e.getMessage(), token, seq, socket);
		}

	}

	private void handleAbortTransaction(AbortTransaction abrt, String token,
			int seq, SocketChannel socket) {
		if (!abrt.hasTransId()) {
			// this is from an internal message, cancel out all transactions
			// from this gateway
			LOG.debug("Aborting transactions for non-responsive gateway "
					+ socket.socket().getInetAddress().getHostName());
			Iterator<Entry<Integer, SyncTransaction>> it = transactions
					.entrySet().iterator();
			while (it.hasNext()) {
				Entry<Integer, SyncTransaction> e = it.next();

				LOG.debug("Aborting transaction tid=" + e.getKey());
				e.getValue().cancel();
				// transactions.remove(e.getKey());
				it.remove();
			}

		} else {
			// TODO: handle abort of individual transaction
			transactions.get(abrt.getTransId()).cancel();
		}
	}

	private void handlePing(SocketChannel socket) {
		gm.handlePing(socket);
	}

	private void sendTableNotFound(String app, String tbl, String token,
			int seq, SocketChannel socket) {
		LOG.error("Table " + app + "." + tbl + " not found");
		SyncResponse.Builder notfound = SyncResponse.newBuilder()
				.setResult(SyncResponse.SyncResult.TABLE_NOT_FOUND).setApp(app)
				.setTbl(tbl).setTransId(seq);
		SimbaMessage m = SimbaMessage.newBuilder().setSeq(seq)
				.setType(SimbaMessage.Type.SYNC_RESPONSE)
				.setSyncResponse(notfound).setToken(token).build();
		server.send(socket, m.toByteString().toByteArray());
	}

	private void handleNewTransaction(SyncRequest msg, String token, int seq,
			SocketChannel socket) {

		LOG.debug("new transaction transId=" + msg.getData().getTransId());
		// add transaction logic here
		if (transactions.containsKey(msg.getData().getTransId())) {
			// this case should not happen...
			LOG.error("Transaction ID already exists in transactions map!");
			sendOperationResponse(false,
					"Cannot create new transaction; already exists!", token,
					seq, socket);
		} else {
			SimbaTable table = findOrRecover(msg.getData().getApp(), msg
					.getData().getTbl());
			if (table == null) {
				sendTableNotFound(msg.getData().getApp(), msg.getData()
						.getTbl(), token, seq, socket);
				return;
			}
			SyncTransaction st = new SyncTransaction(this.server,
					this.objectStore, table, msg.getData(), token, seq, msg
							.getData().getTransId(), socket);
			if (!st.isComplete()) {
				// need to wait for objects
				transactions.put(msg.getData().getTransId(), st);
			}
		}
	}

	private void handleObjectFragment(ObjectFragment msg, String token,
			int seq, SocketChannel socket) {
		// check if transaction exists
		if (transactions.containsKey(msg.getTransId())) {
			// Add fragment to object list
			SyncTransaction st = transactions.get(msg.getTransId());
			st.addFragment(msg);
			if (st.isComplete()) {
				transactions.remove(st);
			}
		} else {
			// this transaction doesn't exist?
			LOG.error("Transaction " + msg.getTransId() + " does not exist!");

			// send error reply message
			sendOperationResponse(false, "Transaction does not exist!", token,
					seq, socket);
		}
	}

	private void handleRequestClientSubscriptions(
			RequestClientSubscriptions requestClientSubscriptions,
			String token, int seq, SocketChannel socket) {

		RestoreClientSubscriptions.Builder rcs = RestoreClientSubscriptions
				.newBuilder();

		ResultSet rowset = tableStore.getSubscriptions(
				requestClientSubscriptions.getClientId(), ConsistencyLevel.ALL);

		if (!rowset.isExhausted()) {
			Row row = rowset.one();

			List<ByteBuffer> subs = row.getList("subscriptions",
					ByteBuffer.class);
			for (ByteBuffer sub : subs) {
				ClientSubscription csub = null;
				try {
					csub = ClientSubscription.parseFrom(sub.array());
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}
				rcs.addSub(csub);
			}
		}

		rcs.setClientId(requestClientSubscriptions.getClientId());
		rcs.build();

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.RESTORE_CLIENT_SUBSCRIPTIONS)
				.setRestoreClientSubscriptions(rcs).build();

		server.send(socket, msg.toByteString().toByteArray());
	}

	private void sendOperationResponse(boolean succeeded, String msg,
			String token, int seq, SocketChannel socket) {
		LOG.debug("Sending operation response; seq=" + seq + " token=" + token
				+ " status=" + succeeded);
		OperationResponse.Builder b = OperationResponse.newBuilder()
				.setStatus(succeeded).setMsg(msg);

		LOG.debug("sendOperationResponse seq=" + seq);

		SimbaMessage m = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.OP_RESPONSE).setSeq(seq)
				.setToken(token).setOperationResponse(b).build();

		server.send(socket, m.toByteString().toByteArray());
	}

	private void handleSaveClientSubscription(
			SaveClientSubscription saveClientSubscription, String token,
			int seq, SocketChannel socket) {
		// append new subscription to list as bytes
		tableStore.putSubscription("simbastore", "subscriptions",
				saveClientSubscription.getClientId(),
				ByteBuffer.wrap(saveClientSubscription.getSub().toByteArray()),
				ConsistencyLevel.ALL);

		// TODO: send response
	}

	private void handleTornRow(TornRowRequest m, String token, int seq,
			SocketChannel socket) {

		SyncHeader rsp = null;

		Integer object_counter = 0;
		LinkedList<List<UUID>> objects = new LinkedList<List<UUID>>();

		SimbaTable table = findOrRecover(m.getApp(), m.getTbl());
		String tablename = m.getApp() + "." + m.getTbl();

		if (table == null) {
			LOG.error("Table " + tablename + " does not exist!");

			// rsp =
			// SyncHeader.newBuilder().setApp(m.getApp()).setTbl(m.getTbl())
			// .setTransId(seq).build();

			sendOperationResponse(false, "Table " + tablename + " not found",
					token, seq, socket);
			return;

		}

		List<DataRow> dirty = new LinkedList<DataRow>();
		List<DataRow> deleted = new LinkedList<DataRow>();

		List<Column> schema = table.getSchema();

		List<String> tornRows = m.getIdList();
		for (String row_id : tornRows) {

			LOG.debug("GET ROW: " + row_id);
			Row row = table.getRow(row_id);

			if (row != null) {
				DataRow dataRow = null;
				try {
					dataRow = Utils.rowToDataRow(row, true, object_counter,
							objects, schema);
				} catch (IOException e) {
					LOG.error("Could not convert row to DataRow message!");
				}

				if (row.getBool("deleted")) {
					// if delete flag is true, add to deleted rows list
					deleted.add(dataRow);
				} else {
					// else, put in dirty rows lists
					dirty.add(dataRow);
				}
			} else {
				LOG.error("PullData: Rowkey (" + row_id + ") does not exist!");
			}
		}

		rsp = SyncHeader.newBuilder().setApp(m.getApp()).setTbl(m.getTbl())
				.addAllDirtyRows(dirty).addAllDeletedRows(deleted)
				.setTransId(seq).build();

		// Send PullData response message
		TornRowResponse trr = TornRowResponse.newBuilder().setData(rsp).build();

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.TORN_RESPONSE)
				.setTornRowResponse(trr).setToken(token).setSeq(seq).build();

		LOG.debug("Sending TORN_RESPONSE message, seq=" + seq);
		server.send(socket, msg.toByteString().toByteArray());

		object_counter = 0; // reset to zero

		// Pull objects from swift
		for (List<UUID> chunks : objects) {
			LOG.debug("Sending fragments for " + objects.size() + " objects");

			for (int i = 0; i < chunks.size(); i++) {
				UUID key = chunks.get(i);

				LOG.debug("Object " + object_counter + ", " + chunks.size()
						+ " fragments.");
				byte[] data = objectStore.getObject(key.toString());

				if (data == null) {
					LOG.debug("Object " + key.toString() + " was not found!");
					// TODO: send error to client?
				} else {
					System.out.println(key + ": " + data.length + " bytes");
				}

				ObjectFragment f = ObjectFragment.newBuilder()
						.setData(ByteString.copyFrom(data))
						.setOid(object_counter).setTransId(seq).setOffset(i)
						.setEof((i == chunks.size() - 1) ? true : false)
						.build();

				// Send object fragments
				SimbaMessage objmsg = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
						.setObjectFragment(f).setToken(token).setSeq(seq)
						.build();

				server.send(socket, objmsg.toByteString().toByteArray());
			}
			object_counter++;
		}
	}

	private void handleCreateTable(CreateTable ct, String token, int seq,
			SocketChannel socket) {

		String newTable = ct.getApp() + "." + ct.getTbl();
		System.out.printf("Create Table --> %s \n", newTable);

		tableWrite.lock();
		try {
			SimbaTable table = findOrRecover(ct.getApp(), ct.getTbl());
			LOG.debug("handleCreateTable seq=" + seq);

			if (table != null) {
				LOG.error("Cannot create table " + newTable
						+ " : already exists!");
				sendOperationResponse(true,
						"table not created; already exists", token, seq, socket);
			} else {

				// table doesn't already exist
				table = SimbaTable.create(tableStore, objectStore,
						subscriptionManager, properties, ct);
				String tablename = table.getId();

				tables.put(tablename, table);

				LOG.info("createTable: created: " + newTable);
				sendOperationResponse(true, "table created", token, seq, socket);
			}
		} finally {
			tableWrite.unlock();
		}
	}

	private void handleDropTable(DropTable m, String token, int seq,
			SocketChannel socket) {
		String tablename = m.getApp() + "." + m.getTbl();

		SimbaTable table = tables.get(tablename);
		if (table == null) {
			sendOperationResponse(false, "table not found", token, seq, socket);
			return;
		}

		table.deleteTable();
		table = null; // remove reference to SimbaTable, GC later
		tables.remove(tablename);

		// TODO: assuming delete operation will succeed. need to determine how
		// to check if cassandra operation has succeeded
		sendOperationResponse(true, "table dropped", token, seq, socket);
	}

	private SimbaTable findOrRecover(String app, String tbl) {
		String tableName = app + "." + tbl;
		SimbaTable table;

		tableRead.lock();
		try {
			table = tables.get(tableName);
			if (table == null) {
				tableRead.unlock();
				tableWrite.lock();
				try {
					table = SimbaTable.restore(tableStore, objectStore,
							subscriptionManager, properties, app, tbl);
					if (table != null)
						tables.put(tableName, table);
				} finally {
					tableRead.lock();
					tableWrite.unlock();
				}
			}
		} finally {
			tableRead.unlock();
		}

		return table;
	}

	private void handleSubscribeTable(SubscribeTable m, String token,
			Integer seq, SocketChannel socket) {

		String tablename = m.getApp() + "." + m.getTbl();
		SimbaTable table = findOrRecover(m.getApp(), m.getTbl());

		// check if table exists
		if (table == null) {
			LOG.error("table " + tablename + " doesn't exist");
			sendOperationResponse(false, "table not found", token, seq, socket);
			return;
		}

		// If no previous subscription to this table, mark it subscribed
		if (!subscriptionManager.contains(tablename)) {

			table.subscribed = true;
		}

		// Add the new subscription
		subscriptionManager.subscribe(tablename, socket);

		LOG.debug("Sending SUB_RESPONSE for " + tablename + ", SEQ=" + seq);

		// send schema
		SubscribeResponse sr = SubscribeResponse.newBuilder()
				.addAllColumns(table.getSchema())
				.setVersion(table.getVersion()).build();

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.SUB_RESPONSE)
				.setSubscribeResponse(sr).setToken(token).setSeq(seq).build();

		server.send(socket, msg.toByteString().toByteArray());

		// send table version
		TableVersion tv = TableVersion.newBuilder().setTable(tablename)
				.setVersion(table.getVersion()).build();

		SimbaMessage msg2 = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.TABLE_VERSION).setTableVersion(tv)
				.build();

		server.send(socket, msg2.toByteString().toByteArray());
	}

	private void handleUnsubscribeTable(UnsubscribeTable m, String token,
			int seq, SocketChannel socket) {
		String tablename = m.getApp() + "." + m.getTbl();

		// Remove the subscription
		subscriptionManager.unsubscribe(tablename, socket);

		// If no more subscriptions for this table, remove subscribed mark
		if (!subscriptionManager.contains(tablename)) {
			SimbaTable table = tables.get(tablename);
			if (table != null) {
				table.subscribed = false;
			}
		}

	}

	private void handleNotifyPull(NotificationPull m, String token, int seq,
			SocketChannel socket) {
		SimbaTable table = findOrRecover(m.getApp(), m.getTbl());
		String tablename = m.getApp() + "." + m.getTbl();
		if (table == null) {
			LOG.error("Table " + tablename + " does not exist!");
			sendOperationResponse(false, "Table " + tablename + " not found",
					token, seq, socket);
			return;
		}

		List<DataRow> dirty = new LinkedList<DataRow>();
		List<DataRow> deleted = new LinkedList<DataRow>();

		Integer object_counter = 0;
		List<Set<Entry<Integer, UUID>>> objects = new LinkedList<Set<Entry<Integer, UUID>>>();
		List<Column> schema = table.getSchema();

		// get row ids for all update versions in the cache
		SortedMap<Integer, String> rows = table.getRowsInVersionRange(
				m.getFromVersion() + 1, m.getToVersion());

		// list of rowkey's already handled
		List<String> completed_keys = new LinkedList<String>();

		LOG.debug("cached rows: {");
		Iterator<Entry<Integer, String>> itr = rows.entrySet().iterator();

		while (itr.hasNext()) {
			Entry<Integer, String> next = itr.next();
			LOG.debug("[" + next.getKey() + "," + next.getValue() + "]");

		}
		LOG.debug("}");

		// check if change log contains all versions in this pull request
		// if it does not, then the change log has insufficient information
		// for this notification pull request and we must read the entire
		// row/objects
		if (table.cache.containsRange(m.getFromVersion() + 1, m.getToVersion())) {

			// just get the changes

			for (int i = m.getFromVersion() + 1; i <= m.getToVersion(); i++) {

				// get row id
				String rowkey = rows.get(i);

				// check if this row has already been handled
				if (completed_keys.contains(rowkey)) {
					continue;
				}

				Row row = null;

				// get row from cassandra
				Long start = System.nanoTime();
				row = table.getRow(rowkey);
				BackendStats
						.logCassandraReadLatency(
								seq,
								((double) System.nanoTime() - (double) start) / 1000000);

				LOG.debug("Getting changeset for versions=["
						+ (m.getFromVersion() + 1) + "," + m.getToVersion()
						+ "]");

				// get changes for this row
				ChangeSet changes = table.cache.getChanges(rowkey,
						m.getFromVersion() + 1, m.getToVersion());

				DataRow.Builder changedRow = DataRow.newBuilder();

				// System.out.println(changes);
				changedRow = DataRow.newBuilder();
				changedRow.setId(rowkey).setRev(changes.version);

				// get table changes
				LOG.debug("Getting changes for columns: "
						+ changes.table_changes);
				for (String s : changes.table_changes) {
					try {
						changedRow.addData(Utils.decodeColumn(s,
								table.getColumnType(s), row));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				// get chunk changes
				for (ObjectChanges oc : changes.object_changes) {
					if (oc.size != null) {
						changedRow.addObj(ObjectHeader.newBuilder()
								.setColumn(oc.name).setOid(object_counter)
								.setNumChunks(oc.size).build());
					} else {
						changedRow.addObj(ObjectHeader.newBuilder()
								.setColumn(oc.name).setOid(object_counter)
								.build());
					}

					objects.add(oc.chunks.entrySet());
					object_counter++;
				}

				DataRow changed = changedRow.build();

				if (row.getBool("deleted")) {
					// if delete flag is true, add to deleted rows list
					deleted.add(changed);
				} else {
					// else, put in dirty rows list
					dirty.add(changed);
				}

				BackendStats.logCassandraReadBytes(seq,
						changed.getSerializedSize());

				completed_keys.add(rowkey);
			}

		} else {

			// get the entire row

			for (int i = m.getFromVersion() + 1; i <= m.getToVersion(); i++) {

				Row row = null;
				String rowkey = null;
				if (rows.containsKey(i)) {
					// cache contains rowkey for this version
					rowkey = rows.get(i);

					Long start = System.nanoTime();
					row = table.getRow(rowkey);
					BackendStats
							.logCassandraReadLatency(
									seq,
									((double) System.nanoTime() - (double) start) / 1000000);
				} else {
					// cache DOES NOT contain rowkey for this version

					Long start = System.nanoTime();
					row = table.getRowByVersion(i);
					BackendStats
							.logCassandraReadLatency(
									seq,
									((double) System.nanoTime() - (double) start) / 1000000);

					if (row == null) {
						LOG.debug(table.getKeyspace() + "." + table.getTable()
								+ ", version=" + i + " does NOT exist!");
						continue;
					}
					rowkey = row.getString("key");
				}

				// check if this row has already been handled
				if (completed_keys.contains(rowkey)) {
					continue;
				}

				if (row != null) {
					DataRow dataRow = null;
					try {
						dataRow = Utils.rowToDataRow2(row, true,
								object_counter, objects, schema);

						BackendStats.logCassandraReadBytes(seq,
								dataRow.getSerializedSize());

					} catch (IOException e) {
						LOG.error("Could not convert row to DataRow message!");
					}

					if (row.getBool("deleted")) {
						// if delete flag is true, add to deleted rows list
						deleted.add(dataRow);
					} else {
						// else, put in dirty rows list
						dirty.add(dataRow);
					}
				} else {
					LOG.error("NotifyPull: Rowkey (" + rowkey
							+ ") does not exist!");
				}

				completed_keys.add(rowkey);
			}

		}

		// Send PullData response message
		SyncHeader h = SyncHeader.newBuilder().setApp(m.getApp())
				.setTbl(m.getTbl()).addAllDirtyRows(dirty)
				.addAllDeletedRows(deleted).setTransId(seq).build();

		PullData p = PullData.newBuilder().setData(h).build();

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.PULL_DATA).setPullData(p)
				.setToken(token).setSeq(seq).build();

		server.send(socket, msg.toByteString().toByteArray());

		object_counter = 0; // reset to zero

		// Pull objects from swift
		for (Set<Entry<Integer, UUID>> chunks : objects) {

			Iterator<Entry<Integer, UUID>> it = chunks.iterator();
			while (it.hasNext()) {

				Entry<Integer, UUID> chunk = it.next();

				Long start = System.nanoTime();
				byte[] data = objectStore
						.getObject(chunk.getValue().toString());
				BackendStats.logSwiftReadBytes(seq, data.length);
				BackendStats
						.logSwiftReadLatency(
								seq,
								((double) System.nanoTime() - (double) start) / 1000000);

				LOG.debug("GET OBJECT " + chunk.getValue() + ": " + data.length
						+ " bytes");

				int objseq = sequencer.getSeq();

				// Split objects into fragments
				ObjectFragment f = ObjectFragment.newBuilder().setTransId(seq)
						.setData(ByteString.copyFrom(data))
						.setOid(object_counter).setOffset(chunk.getKey())
						.setEof((!it.hasNext()) ? true : false).build();
				// );
				SimbaMessage objmsg = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
						.setObjectFragment(f).setToken(token).setSeq(objseq)
						.build();

				server.send(socket, objmsg.toByteString().toByteArray());
			}
			object_counter++;
		}

		BackendStats.writeRecord(seq);
	}

	private void handlePullData(PullData m, String token, int seq,
			SocketChannel socket) {
		SyncHeader d = m.getData();

		SyncHeader rsp = null;

		int object_counter = 0;
		LinkedList<List<UUID>> objects = new LinkedList<List<UUID>>();

		SimbaTable table = findOrRecover(d.getApp(), d.getTbl());
		String tablename = d.getApp() + "." + d.getTbl();
		if (table == null) {
			LOG.error("Table " + tablename + " does not exist!");

			sendOperationResponse(false, "Table " + tablename + " not found",
					token, seq, socket);
			return;
		}

		List<DataRow> dirty = new LinkedList<DataRow>();
		List<DataRow> deleted = new LinkedList<DataRow>();

		List<Column> schema = table.getSchema();

		List<DataRow> pullRows = d.getDirtyRowsList();
		for (DataRow getRow : pullRows) {
			LOG.debug("GET ROW: " + getRow.getId());
			Row row = table.getRow(getRow.getId());

			if (row != null) {
				DataRow dataRow = null;
				try {
					dataRow = Utils.rowToDataRow(row, true, object_counter,
							objects, schema);
				} catch (IOException e) {
					LOG.error("Could not convert row to DataRow message!");
				}

				if (row.getBool("deleted")) {
					// if delete flag is true, add to deleted rows list
					deleted.add(dataRow);
				} else {
					// else, put in dirty rows list
					dirty.add(dataRow);
				}
			} else {
				// TODO: not sure how to handle an error caused by a missing
				// row. However, a response will be send regardless.
				LOG.error("PullData: Rowkey (" + getRow.getId()
						+ ") does not exist!");
			}
		}

		rsp = SyncHeader.newBuilder().setApp(d.getApp()).setTbl(d.getTbl())
				.addAllDirtyRows(dirty).addAllDeletedRows(deleted)
				.setTransId(seq).build();

		// Send PullData response message
		PullData p = PullData.newBuilder().setData(rsp).build();

		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.PULL_DATA).setPullData(p)
				.setToken(token).setSeq(seq).build();

		LOG.debug("Sending PULL_DATA message, seq=" + seq);
		server.send(socket, msg.toByteString().toByteArray());

		object_counter = 0; // reset to zero

		// Pull objects from swift
		for (List<UUID> chunks : objects) {
			LOG.debug("Sending fragments for " + objects.size() + " objects");

			for (int i = 0; i < chunks.size(); i++) {
				// for (UUID key : chunks) {
				UUID key = chunks.get(i);

				LOG.debug("Object " + object_counter + ", " + chunks.size()
						+ " fragments.");
				Long start = System.nanoTime();
				byte[] data = objectStore.getObject(key.toString());
				System.out.println("GETOBJECT "
						+ ((double) System.nanoTime() - (double) start)
						/ 1000000);
				// System.out.println(key + ": " + data.length + " bytes");

				ObjectFragment f = ObjectFragment.newBuilder()
						.setData(ByteString.copyFrom(data))
						.setOid(object_counter).setTransId(seq).setOffset(i)
						.setEof((i == chunks.size() - 1) ? true : false)
						.build();

				// Send object fragment
				SimbaMessage objmsg = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
						.setObjectFragment(f).setToken(token).setSeq(seq)
						.build();

				server.send(socket, objmsg.toByteString().toByteArray());

			}
			object_counter++;
		}
	}

	public GatewayManager getGatewayManager() {
		return this.gm;
	}
}
