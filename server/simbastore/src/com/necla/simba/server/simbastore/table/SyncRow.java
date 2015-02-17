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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.SimbaConsistency;
import com.necla.simba.server.simbastore.table.SyncRow;
import com.necla.simba.server.simbastore.table.SyncTransaction;
import com.necla.simba.server.simbastore.cache.ChangeSet;
import com.necla.simba.server.simbastore.cache.ObjectChanges;
import com.necla.simba.server.simbastore.stats.BackendStats;
import com.necla.simba.server.simbastore.swift.SwiftHandler;
import com.necla.simba.server.simbastore.util.Utils;
import com.necla.simba.util.Pair;

/**
 * @author Dorian Perkins <dperkins@nec-labs.com>
 * @created Aug 11, 2013 5:33:08 PM
 */
public class SyncRow {
	private static final Logger LOG = LoggerFactory.getLogger(SyncRow.class);

	Row oldRow = null;
	DataRow newRow;

	Integer row_object_count = 0;

	boolean conflict = false;

	SyncTransaction st;

	HashMap<Integer, List<UUID>> oldObjects;
	HashMap<Integer, LinkedList<Pair<Integer, UUID>>> newObjects;
	private SwiftHandler objectStore;

	Long start;

	public SyncRow(SwiftHandler objectStore, DataRow row,
			Integer row_object_count, SyncTransaction st, boolean delete) {

		this.st = st;
		this.objectStore = objectStore;
		this.newRow = row;
		this.row_object_count = row_object_count;

		start = System.nanoTime();
		this.oldRow = st.table.getRow(row.getId());
		BackendStats.logCassandraReadLatency(st.tid,
				((double) System.nanoTime() - (double) start) / 1000000);

		if (this.st == null) {
			System.err.println("SyncTransaction (st) is NULL!");
		}

		if (!this.st.table.getConsistencyLevel().equals(
				SimbaConsistency.Type.EVENTUAL)) {

			if (oldRow == null) {
				LOG.debug("ROW '" + newRow.getId()
						+ "' does not exist in TABLE '"
						+ st.table.getKeyspace() + "." + st.table.getTable()
						+ "'");
			}

			/*
			 * Unless SimbaConsistency level is EVENTUAL, check for conflicts.
			 */
			conflict = checkConflict();
		}

		if (!conflict) {
			// VERSION IS OK!

			st.table.operationLog.write(st.tid + " " + newRow.getId()
					+ " START_TRANSACTION "
					+ ((oldRow == null) ? -1 : oldRow.getInt("version")));

			if (delete) {
				// LOG DELETED ROW
				st.table.operationLog.write(st.tid + " " + newRow.getId()
						+ " DEL ");

				// the only exception that can occur is row doesn't exist
				// however this should not be possible unless we try to delete
				// a row that truly never existed.
				if (oldRow != null) {
					st.table.markDeleted(newRow);
				} else {
					// Row never existed (or has been removed by GC)
					LOG.error("Row to delete does not exist: " + newRow.getId());
				}

				DataRow deleted = DataRow.newBuilder().setId(newRow.getId())
						.setRev(-1).build();

				st.syncedRows.add(deleted);

				st.table.operationLog.write(st.tid + " " + newRow.getId()
						+ " END_TRANSACTION");

				System.out.println(newRow.getId());
				st.finishRow(this);

			} else {
				// at this point we only know which objects are going to have
				// changes but we do not know which chunks of these objects are
				// being changed since we do not have any object fragments.

				if (newRow.getObjCount() > 0) {

					if (oldRow != null) {

						// copy the chunk UUIDs for objects which have changes
						for (ObjectHeader obj : newRow.getObjList()) {

							try {
								if (oldObjects == null) {
									oldObjects = new HashMap<Integer, List<UUID>>();
								}
								LOG.debug("Reading chunks for col="
										+ obj.getColumn());
								List<UUID> old = oldRow.getList(
										obj.getColumn(), UUID.class);

								if (old != null) {
									LOG.debug("OBJECT " + obj.getColumn()
											+ " from ROW '" + newRow.getId()
											+ "' in TABLE '"
											+ st.table.getKeyspace() + "."
											+ st.table.getTable()
											+ "' will be overwritten");
									LOG.debug(obj.getColumn() + "=" + old);
									oldObjects.put(obj.getOid(), old);
								}

								// st.table.operationLog.write(row_tid +
								// "obj UUID");

							} catch (IllegalArgumentException e) {
								continue;
							}
						}
					}
				} else {
					// no objects for this update/insert, so call PutRow
					LOG.debug("No objects. Calling PutRow");
					st.table.putRow(newRow, null, null, st.syncedRows, st.tid);
					st.table.operationLog.write(st.tid + " " + newRow.getId()
							+ " END_TRANSACTION");
					LOG.debug("Calling FinishRow");
					System.out.println(newRow.getId());
					st.finishRow(this);
				}
			}
		} else {
			// conflict == true
			if (newRow.getObjCount() == 0) {
				// only call finish row if no objects for this row
				LOG.debug("Conflict row (" + newRow.getId()
						+ ") with no objects!");
				System.out.println(newRow.getId());
				st.finishRow(this);
			} else {
				LOG.debug("Conflict row (" + newRow.getId() + ") with "
						+ newRow.getObjCount() + " objects!");
			}
		}

	}
	
	public void addFragment(ObjectFragment f) {

		if (!conflict) {
			// generate new UUID for chunk
			final UUID uuid = UUID.randomUUID();

			// log message = TID# NEW OID OFFSET UUID
			st.table.operationLog.write(st.tid + " " + newRow.getId() + " NEW "
					+ f.getOid() + " " + f.getOffset() + " " + uuid);

			// write to swift
			Long start = System.nanoTime();
			objectStore.putObject(uuid.toString(), f.getData().toByteArray());
			BackendStats.logSwiftWriteLatency(st.tid,
					((double) System.nanoTime() - (double) start) / 1000000);
			BackendStats
					.logSwiftWriteBytes(st.tid, f.getData().toByteArray().length);
			System.out.println("PUTOBJECT "
					+ ((double) System.nanoTime() - (double) start) / 1000000);
			LOG.debug("PUT FRAGMENT '" + uuid + "' ("
					+ f.getData().toByteArray().length + " bytes, offset="
					+ f.getOffset() + ") (TABLE: " + st.table.getKeyspace()
					+ "." + st.table.getTable() + ")");

			// create new object map if does not exist
			if (newObjects == null) {
				newObjects = new HashMap<Integer, LinkedList<Pair<Integer, UUID>>>();
			}

			Pair<Integer, UUID> chunk = new Pair<Integer, UUID>(f.getOffset(),
					uuid);

			// create new chunk list for object does not exist
			LinkedList<Pair<Integer, UUID>> list = null;
			if (!newObjects.containsKey(f.getOid())) {
				list = new LinkedList<Pair<Integer, UUID>>();
				newObjects.put(f.getOid(), list);
			} else {
				list = newObjects.get(f.getOid());
			}

			// add new chunk id to object list
			list.add(chunk);
		}

		if (f.getEof() == true) {
			row_object_count--;
		}

		if (row_object_count == 0) {
			if (!conflict) {
				// write this row
				st.table.putRow(newRow, oldObjects, newObjects, st.syncedRows,
						st.tid);
			}

			st.table.operationLog.write(st.tid + " " + newRow.getId()
					+ " END_TRANSACTION");

			System.out.println(newRow.getId());
			st.finishRow(this);

		}
	}

	private boolean checkConflict() {
		Integer currentVersion = null;

		if (oldRow != null) {
			currentVersion = oldRow.getInt("version");

			if (oldRow.getBool("deleted")) {
				if (newRow.getRev() == -1) {
					// client knows row is deleted. wants to resolve conflict by
					// updating row.
					return false;
				} else {
					// CHECK DELETED
					// IF DELETED COLUMN SET TRUE, RETURN TRUE (delete-update
					// conflict)
					LOG.error("putRow failed: delete-update conflict: "
							+ newRow.getId());

					DataRow deleted = DataRow.newBuilder()
							.setId(newRow.getId()).setRev(-1).build();

					st.conflictRows.add(deleted);
					return true;
				}
			} else if (currentVersion != newRow.getRev()) {
				// CHECK VERSION
				LOG.error("putRow failed: version conflict: " + newRow.getId()
						+ ": " + currentVersion + " != " + newRow.getRev());

				// if operation cache contains all versions between fromVersion
				// and toVersion return changes between client's version and
				// current row version
				if (st.table.cache.containsRange(newRow.getRev() + 1,
						currentVersion)) {

					DataRow.Builder changedRow = DataRow.newBuilder();
					// get changes for this row
					ChangeSet changes = st.table.cache
							.getChanges(newRow.getId(), newRow.getRev() + 1,
									currentVersion);

					changedRow = DataRow.newBuilder();
					changedRow.setId(newRow.getId()).setRev(currentVersion);

					for (String s : changes.table_changes) {
						try {
							changedRow.addData(Utils.decodeColumn(s,
									st.table.getColumnType(s), oldRow));
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					// get chunk changes
					for (ObjectChanges oc : changes.object_changes) {
						if (oc.size != null) {
							changedRow.addObj(ObjectHeader.newBuilder()
									.setColumn(oc.name)
									.setOid(st.conflict_object_counter)
									.setNumChunks(oc.size).build());
						} else {
							changedRow
									.addObj(ObjectHeader.newBuilder()
											.setColumn(oc.name)
											.setOid(st.conflict_object_counter)
											.build());
						}
						st.conflict_objects.add(oc.chunks.entrySet());
						st.conflict_object_counter++;
					}

					st.conflictRows.add(changedRow.build());
					return true;

				} else {
					DataRow conflict = null;

					try {
						conflict = Utils.rowToDataRow2(oldRow, true,
								st.conflict_object_counter,
								st.conflict_objects, st.table.getSchema());
					} catch (IOException e) {
						e.printStackTrace();
					}
					st.conflictRows.add(conflict);

				}
				return true;

			}
		}
		return false;
	}

	public void cancel() {
		// need to clean up persistent state left over by the SyncRow

		LOG.debug("Canceling SYNC on row=" + newRow.getId());

		// DELETE ALL NEW CHUNKS
		if (newObjects != null) {
			Iterator<Entry<Integer, LinkedList<Pair<Integer, UUID>>>> it = newObjects
					.entrySet().iterator();

			while (it.hasNext()) {
				Entry<Integer, LinkedList<Pair<Integer, UUID>>> e = it.next();

				LOG.debug("Removing new chunks for objID=" + e.getKey());

				for (Pair<Integer, UUID> chunk : e.getValue()) {
					LOG.debug("Deleting chunk " + chunk.getSecond());
					objectStore.deleteObject(chunk.getSecond().toString());
				}
			}
		}

		st.table.operationLog.write(st.tid + " " + newRow.getId() + " ABORT");
	}
}
