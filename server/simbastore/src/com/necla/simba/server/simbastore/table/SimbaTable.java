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
package com.necla.simba.server.simbastore.table;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.protobuf.InvalidProtocolBufferException;
import com.necla.simba.protocol.Common.Column;
import com.necla.simba.protocol.Common.ColumnData;
import com.necla.simba.protocol.Common.CreateTable;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.SimbaConsistency;
import com.necla.simba.protocol.Common.SimbaConsistency.Type;
import com.necla.simba.protocol.Server.ClientSubscription;
import com.necla.simba.server.simbastore.table.OperationLog;
import com.necla.simba.server.simbastore.table.SimbaTable;
import com.necla.simba.server.simbastore.cache.ChangeSet;
import com.necla.simba.server.simbastore.cache.ObjectChanges;
import com.necla.simba.server.simbastore.cache.OperationCache;
import com.necla.simba.server.simbastore.cassandra.CassandraHandler;
import com.necla.simba.server.simbastore.server.SubscriptionManager;
import com.necla.simba.server.simbastore.stats.BackendStats;
import com.necla.simba.server.simbastore.swift.SwiftHandler;
import com.necla.simba.util.Pair;
import com.necla.simba.util.Utils;

public class SimbaTable {
	private static final Logger LOG = LoggerFactory.getLogger(SimbaTable.class);

	public OperationCache cache;
	OperationLog operationLog;

	private SimbaConsistency.Type consistencyLevel;

	private String keyspace;
	private String table;
	private int version;
	private List<Column> schema;
	private Map<String, Column.Type> schemaMap = new HashMap<String, Column.Type>();

	public boolean subscribed;
	private String id; // id = keyspace.table

	// protected static PrintWriter out;
	private CassandraHandler tableStore;
	private SwiftHandler objectStore;
	private SubscriptionManager sm;
	private Properties props;

	private ConsistencyLevel readConsistency = ConsistencyLevel.ONE;
	private ConsistencyLevel writeConsistency = ConsistencyLevel.ALL;

	private SimbaTable(CassandraHandler tableStore, SwiftHandler objectStore,
			SubscriptionManager sm, Properties props) {
		cache = new OperationCache(props);
		this.version = -1;
		this.tableStore = tableStore;
		this.objectStore = objectStore;
		this.sm = sm;
		this.props = props;
		subscribed = false;

	}

	public static SimbaTable create(CassandraHandler tableStore,
			SwiftHandler objectStore, SubscriptionManager sm, Properties props,
			CreateTable ct) {
		SimbaTable table = new SimbaTable(tableStore, objectStore, sm, props);
		table.doCreate(ct);
		return table;
	}

	public static SimbaTable restore(CassandraHandler tableStore,
			SwiftHandler objectStore, SubscriptionManager sm, Properties props,
			String keyspace, String tableName) {

		List<Column> schema = tableStore.getSchema(keyspace, tableName);
		if (schema == null) {
			LOG.info("Table to restore not found: " + keyspace + "."
					+ tableName);
			return null;
		}
		int version = tableStore.getVersion(keyspace, tableName,
				ConsistencyLevel.ONE);
		SimbaTable table = new SimbaTable(tableStore, objectStore, sm, props);
		table.doRestore(keyspace, tableName, schema, version);

		return table;
	}

	private void doCreate(CreateTable ct) {
		this.keyspace = ct.getApp();
		this.table = ct.getTbl();

		// NA: create keyspace, ignore exception if already exists
		tableStore.createKeyspace(this.keyspace, 1);

		tableStore.createTable(this.keyspace, this.table, ct.getColumnsList());

		// Save schema
		schema = new ArrayList<Column>();
		schema.addAll(ct.getColumnsList());
		for (Column c : ct.getColumnsList())
			this.schemaMap.put(c.getName(), c.getType());

		// Assign this tables id
		this.id = this.keyspace + "." + this.table;

		// Set and save consistency level
		this.consistencyLevel = ct.getConsistencyLevel().getType();
		LOG.error("Creating new table=" + this.id + " with ConsistencyLevel="
				+ this.consistencyLevel);
		setConsistencyLevel(this.id, this.consistencyLevel);

		// create operation log
		operationLog = new OperationLog(this.id, this.props);

	}

	private void getConsistencyLevel(String key) {
		ResultSet rowset = tableStore.getTableConsistencyLevel(key);

		if (!rowset.isExhausted()) {
			Row row = rowset.one();
			this.consistencyLevel = SimbaConsistency.Type.valueOf(row
					.getString("consistency"));
		} else {
			LOG.error("ERROR: MissingConsistencyException: no consistency level set for table: "
					+ key);
		}
	}

	private void setConsistencyLevel(String key,
			SimbaConsistency.Type consistencyLevel) {
		tableStore.setTableConsistencyLevel(key, consistencyLevel);
	}

	private void doRestore(String keyspace, String table, List<Column> schema,
			int version) {
		this.keyspace = keyspace;
		this.table = table;
		this.schema = schema;
		for (Column c : schema)
			this.schemaMap.put(c.getName(), c.getType());

		this.version = version;

		// Assign this tables id
		this.id = this.keyspace + "." + this.table;

		getConsistencyLevel(this.id);

		LOG.debug("Restored table " + this.id + " @ version=" + version);

		// create operation log
		operationLog = new OperationLog(this.id, this.props);
		// TODO: restore old log

	}

	private String generateKey() {
		final String uuid = UUID.randomUUID().toString();
		return uuid;
	}

	private synchronized Integer getNextVersion() {
		version++;
		return version;
	}

	public synchronized void putRow(DataRow newRow,
			HashMap<Integer, List<UUID>> oldObjects,
			HashMap<Integer, LinkedList<Pair<Integer, UUID>>> newObjectMap,
			List<DataRow> syncedRows, int tid) {
		// Double total_time = 0.0;
		// Long start = 0L;

		LOG.debug("PUT ROW: " + newRow.getId() + " (TABLE: " + this.keyspace
				+ "." + this.table + ")");

		if (keyspace == null || table == null) {
			LOG.error("putRow failed: keyspace or table name is NULL!");
			return;
		}

		// 2. PERFORM SET STEP

		// 2.1 PUT NEW OBJECT DATA

		// intialize change set
		ChangeSet change = new ChangeSet();
		List<ObjectChanges> object_changes = null;

		// 2.1.1 generate object ids
		List<ColumnData> newObjects = null;
		// HashMap<Integer, String> newObjMap = null;
		List<UUID> chunksToDelete = null;

		// operationLog.write("NEW_OBJECTS");
		for (ObjectHeader obj : newRow.getObjList()) {
			if (newObjects == null) {
				newObjects = new LinkedList<ColumnData>();
			}

			if (chunksToDelete == null) {
				chunksToDelete = new LinkedList<UUID>();
			}

			if (object_changes == null) {
				object_changes = new LinkedList<ObjectChanges>();
			}

			// replace the uuids at the offset index within the old object chunk
			// list with the uuids of the new chunks
			ColumnData object = null;

			// change log stuff
			int chunk_index_counter = 0;
			ObjectChanges objchange = new ObjectChanges(obj.getColumn());

			if (oldObjects != null) {
				// old objects exist

				List<UUID> oldObject = oldObjects.get(obj.getOid());

				LOG.debug("Old object num chunks=" + oldObject.size());

				// check if the object needs to be truncated
				if (obj.getNumChunks() < oldObject.size()) {

					// mark the truncated chunks for deletion
					chunksToDelete.addAll(oldObject.subList(obj.getNumChunks(),
							oldObject.size()));

					// truncate the list
					oldObject = oldObject.subList(0, obj.getNumChunks());

					LOG.debug("post-truncate num chunks=" + oldObject.size());
				}

				// process new chunks
				for (Pair<Integer, UUID> chunk : newObjectMap.get(obj.getOid())) {
					// check if any chunks are being replaced, else, just add
					// the new chunks in orders

					// TODO: this logic is odd...fix it
					LOG.debug("chunk=<" + chunk.getFirst() + ", "
							+ chunk.getSecond() + ">");

					if (oldObject == null || oldObject.size() == 0) {
						oldObject = new LinkedList<UUID>();
					}

					if (oldObject.size() < chunk.getFirst() + 1) {
						// add new chunks

						// assuming incoming chunks are ordered
						oldObject.add(chunk.getSecond());

					} else {
						// replace chunks

						// mark old chunk to be deleted
						chunksToDelete.add(oldObject.get(chunk.getFirst()));

						// insert new chunk in its place
						oldObject.set(chunk.getFirst(), chunk.getSecond());

						// add to recovery log
						// TODO: I need the row transaction id...
						operationLog.write(tid + " " + newRow.getId() + " OLD "
								+ obj.getColumn() + " " + chunk.getFirst()
								+ " " + chunk.getSecond());
					}

					// add to change log
					objchange.chunks.put(chunk.getFirst(), chunk.getSecond());
				}

				// record object size in chunks change log
				objchange.size = oldObject.size();

				// build column to insert into cassandra
				object = ColumnData.newBuilder().setColumn(obj.getColumn())
						.setValue(Utils.chunkListToString(oldObject)).build();
			} else {
				// no old objects exist

				List<UUID> chunks = new LinkedList<UUID>();
				for (Pair<Integer, UUID> chunk : newObjectMap.get(obj.getOid())) {
					chunks.add(chunk.getSecond());

					// add to change log
					objchange.chunks
							.put(chunk_index_counter, chunk.getSecond());
					chunk_index_counter++;
				}

				// record object size in chunks change log
				objchange.size = chunks.size();

				// build column to insert into cassandra
				object = ColumnData.newBuilder().setColumn(obj.getColumn())
						.setValue(Utils.chunkListToString(chunks)).build();
			}

			// add object changes to the change log entry
			change.object_changes.add(objchange);

			// add this column into the new column changes list
			newObjects.add(object);
		}

		// 2.1.2 log transaction data
		// TODO: log changes here

		// log column changes
		for (ColumnData cd : newRow.getDataList()) {
			change.table_changes.add(cd.getColumn());
		}

		/* ****************************************************
		 * ****** NO CASSANDRA CHANGES BEFORE THIS POINT ******
		 * ****************************************************
		 */

		// 2.2 UPDATE VERSION

		// Increment simba table version/set row version
		// getNextVersion() is a synchronized method
		Integer version = getNextVersion();

		// 2.2.1 write version to change log entry
		change.version = version;

		// 2.3 PREPARE ROW DATA TO STORE

		// List<ColumnData> columns = newRow.getDataList();
		List<ColumnData> columns = null;

		if (newObjects != null && !newObjects.isEmpty()) {
			// if there are blobs
			columns = new ArrayList<ColumnData>();
			columns.addAll(newRow.getDataList());
			columns.addAll(newObjects);
		} else {
			// else, no blobs
			columns = newRow.getDataList();
		}

		// 3. PERFORM UPDATE STEP

		// 3.1 Write row to table store

		// start = System.nanoTime();
		Long start = System.nanoTime();
		tableStore.putRow(keyspace, table, newRow.getId(), version, columns,
				writeConsistency);
		BackendStats.logCassandraWriteLatency(tid,
				((double) System.nanoTime() - (double) start) / 1000000);
		BackendStats.logCassandraWriteBytes(tid, newRow.getSerializedSize());
		System.out.println("PUTROW "
				+ ((double) System.nanoTime() - (double) start) / 1000000);
		// total_time += (System.nanoTime() - (double) start) / 1000000;

		LOG.debug("putRow: PUT '" + newRow.getId() + "' version " + version);

		// 3.2 Delete old blob data on overwrite
		if (chunksToDelete != null && !chunksToDelete.isEmpty()) {
			for (UUID chunk : chunksToDelete) {
				LOG.debug("DELETE OBJECT: " + chunk + " (TABLE: "
						+ this.keyspace + "." + this.table + ")");
				// TODO: Catch exceptions here

				start = System.nanoTime();
				objectStore.deleteObject(chunk.toString());
				BackendStats.logSwiftWriteLatency(tid,
						(System.nanoTime() - (double) start) / 1000000);

			}
		}

		// 3.3 Add to cache
		cache.put(version, newRow.getId());
		cache.putChanges(newRow.getId(), version, change);

		// 3.4 Tell subscription manager about new version
		if (subscribed) {
			sm.sendUpdate(id, version);
		}

		// out.println("PUT " + total_time.toString());

		DataRow synced = DataRow.newBuilder().setId(newRow.getId())
				.setRev(version).build();

		syncedRows.add(synced);

		BackendStats.writeRecord(tid);
	}

	public Row getRow(String rowkey) {
		// Long start = System.nanoTime();

		LOG.debug("GET ROW: " + rowkey + " (TABLE: " + this.keyspace + "."
				+ this.table + ")");

		// GET CQL Row
		// TODO: Catch exceptions here
		Long start = System.nanoTime();
		ResultSet results = tableStore.getRow(keyspace, table, rowkey,
				readConsistency);
		System.out.println("GETROW "
				+ ((double) System.nanoTime() - (double) start) / 1000000);
		if (!results.isExhausted()) {
			return results.one();
		} else {
			LOG.error(rowkey + " does not exist in table " + this.id);
			return null;
		}
		// out.println("GET " + (System.nanoTime() - (double) start) / 1000000);
	}

	public Iterator<Row> getRows(String startRowKey, int limit) {
		// Long start = System.nanoTime();
		LOG.debug("GET ROWS: (TABLE: " + this.keyspace + "." + this.table + ")"
				+ " start=" + startRowKey + " limit=" + limit);

		ResultSet results = tableStore.getRows(keyspace, table, limit,
				startRowKey, readConsistency);
		// out.println("GET " + (System.nanoTime() - (double) start) / 1000000);

		if (results == null) {
			LOG.error("ResultSet is null!");
			return null;
		}

		return results.iterator();
	}

	public Row getRowByVersion(int version) {
		// Long start = System.nanoTime();

		LOG.debug("GET ROW where version=" + version + " (TABLE: "
				+ this.keyspace + "." + this.table + ")");

		// GET CQL Row
		// TODO: Catch exceptions here
		ResultSet result = tableStore.getRowByVersion(keyspace, table, version,
				readConsistency);

		// out.println((System.nanoTime() - (double) start) / 1000000);

		if (result == null) {
			LOG.error("ResultSet is null!");
			return null;
		}

		if (result.isExhausted()) {
			return null;
		}

		return result.one();
	}

	public SortedMap<Integer, String> getRowsInVersionRange(Integer lowVersion,
			Integer highVersion) {

		// get all records in this range, excluding the lowVersion
		SortedMap<Integer, String> range = cache.getRange(lowVersion,
				highVersion);

		return range;
	}

	public void markDeleted(DataRow row) {

		// Increment simba table version/set row version
		Integer version = getNextVersion();

		tableStore.markDeleted(keyspace, table, row.getId(), version,
				writeConsistency);

		ChangeSet change = new ChangeSet();
		change.deleted = true;
		change.version = version;

		// Add to cache
		cache.put(version, row.getId());
		cache.putChanges(row.getId(), version, change);

		// Tell subscription manager about new version
		if (subscribed) {
			sm.sendUpdate(id, version);
		}
	}

	public void deleteRow(DataRow row) {

		boolean hasObjects = false;

		// check if row might have object
		for (Column c : schema) {
			if (c.getType().equals("list<uuid>")) {
				hasObjects = true;
				break;
			}
		}

		// if the row might have blob data, fetch the row
		if (hasObjects) {
			// get row to get UUID(s) of blob(s)
			// TODO: Catch exceptions here
			Row result = tableStore.getRow(keyspace, table, row.getId(), null)
					.one();

			if (result != null) {
				// delete blob if exists
				for (Column c : schema) {
					if (c.getType().equals("list<uuid>")) {
						try {
							List<UUID> delete_objects = result.getList(
									c.getName(), UUID.class);
							// UUID toDelete = result.getUUID(c.getName());
							for (UUID toDelete : delete_objects) {
								objectStore.deleteObject(toDelete.toString());
							}
						} catch (IllegalArgumentException e) {
							continue;
						}
					}
				}
			}
		}

		// delete the row
		tableStore.deleteRow(keyspace, table, row.getId(), writeConsistency);
	}

	public void deleteTable() {
		// TODO: delete all objects associated with this table?
		// TODO: Catch exceptions here
		tableStore.dropTable(id);
	}

	public List<Column> getSchema() {
		return this.schema;
	}

	public Column.Type getColumnType(String colName) {
		return this.schemaMap.get(colName);
	}

	public int getVersion() {
		return this.version;
	}

	public String getKeyspace() {
		return this.keyspace;
	}

	public String getTable() {
		return this.table;
	}

	public String getId() {
		return this.id;
	}

	public SimbaConsistency.Type getConsistencyLevel() {
		return this.consistencyLevel;
	}
}
