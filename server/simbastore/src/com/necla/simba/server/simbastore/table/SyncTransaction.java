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

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncResponse;
import com.necla.simba.protocol.Common.SyncResponse.SyncResult;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.simbastore.table.SimbaTable;
import com.necla.simba.server.simbastore.table.SyncRow;
import com.necla.simba.server.simbastore.table.SyncTransaction;
import com.necla.simba.server.simbastore.server.SimbaStoreServer;
import com.necla.simba.server.simbastore.swift.SwiftHandler;

public class SyncTransaction {
	private static final Logger LOG = LoggerFactory
			.getLogger(SyncTransaction.class);

	SyncHeader header;
	private SimbaStoreServer server;
	private SwiftHandler objectStore;
	String token;
	int seq;
	int tid;
	SocketChannel socket;

	SimbaTable table;

	List<DataRow> syncedRows;
	List<DataRow> conflictRows;

	HashMap<Integer, String> oidToRow;

	HashMap<String, SyncRow> rows = new HashMap<String, SyncRow>();

	int rows_remaining = 0;
	int num_objects = 0;
	private boolean completed = false;

	// handle objects for conflicts
	Integer conflict_object_counter = 0;
	List<Set<Entry<Integer, UUID>>> conflict_objects = new LinkedList<Set<Entry<Integer, UUID>>>();

	public SyncTransaction(SimbaStoreServer server,
			SwiftHandler objectStore, SimbaTable table,
			SyncHeader header, String token, int seq, int tid, SocketChannel socket) {
		this.header = header; // copy header message
		this.server = server;
		this.objectStore = objectStore;
		this.token = token;
		this.seq = seq;
		this.tid = tid;
		this.socket = socket;
		this.table = table;

		syncedRows = new LinkedList<DataRow>();
		conflictRows = new LinkedList<DataRow>();

		rows_remaining = header.getDirtyRowsCount()
				+ header.getDeletedRowsCount();

		// calculate number of expected objects for entire transaction
		for (DataRow row : header.getDirtyRowsList()) {
			num_objects += row.getObjCount();
			for (ObjectHeader h : row.getObjList()) {
				if (oidToRow == null) {
					oidToRow = new HashMap<Integer, String>();
				}
				oidToRow.put(h.getOid(), row.getId());
			}
		}

		LOG.debug("New SyncTransaction: (" + tid + ", " + header.getApp() + "."
				+ header.getTbl() + ")");
		LOG.debug("Expecting " + num_objects + " objects");

		for (DataRow row : header.getDirtyRowsList()) {
			// create SyncRow objects
			SyncRow sr = new SyncRow(objectStore, row, row.getObjCount(),
					this, false);

			rows.put(row.getId(), sr);
		}

		for (DataRow row : header.getDeletedRowsList()) {
			// create SyncRow objects
			SyncRow sr = new SyncRow(objectStore, row, row.getObjCount(),
					this, true);

			rows.put(row.getId(), sr);
		}
	}

	/**
	 * Returns the number of objects which have outstanding fragments yet to be
	 * added.
	 */
	public int remainingObjects() {
		return num_objects;
	}

	public void addFragment(ObjectFragment f) {
		// determine which row this fragment belongs to
		String rowId = oidToRow.get(f.getOid());
		SyncRow sr = rows.get(rowId);

		// pass object fragment to that SyncRow
		sr.addFragment(f);
        LOG.debug("fragment oid=" + f.getOid() + " isEOF=" + f.getEof());

		// is this the last fragment of an object?
		if (f.getEof() == true) {
			LOG.debug("Finished object" + f.getOid() + " (" + tid + ", "
					+ header.getApp() + "." + header.getTbl() + ")");
			num_objects--; // decrement number of remaining objects
		}
	}

	public void finishRow(SyncRow row) {
		// decrement the remaining rows counter.
		rows_remaining--;

		// remove row from list
		rows.remove(row);

		LOG.debug("Rows remaining=" + rows_remaining);

		// if no rows remain, complete the transaction
		if (rows_remaining == 0) {
			// trigger sync response
			SyncResponse rsp;
			rsp = SyncResponse.newBuilder().setApp(header.getApp())
					.setTbl(header.getTbl()).setResult(SyncResult.OK)
					.setTransId(tid)
					.addAllSyncedRows(syncedRows)
					.addAllConflictedRows(conflictRows).build();

			LOG.debug("Sending SYNC_RESPONSE for TID#: " + tid);

			SimbaMessage msg = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.SYNC_RESPONSE)
					.setSyncResponse(rsp).setToken(token).setSeq(seq).build();

			server.send(socket, msg.toByteString().toByteArray());

			conflict_object_counter = 0; // reset to zero

			// Pull objects chunk(s) from swift and send
			for (Set<Entry<Integer, UUID>> chunks : conflict_objects) {

				Iterator<Entry<Integer, UUID>> it = chunks.iterator();
				while (it.hasNext()) {

					Entry<Integer, UUID> chunk = it.next();

					byte[] data = objectStore.getObject(chunk.getValue()
							.toString());
					LOG.debug("GET OBJECT " + chunk.getValue() + ": "
							+ data.length + " bytes");

					ObjectFragment f = ObjectFragment.newBuilder()
							.setTransId(tid).setData(ByteString.copyFrom(data))
							.setOid(conflict_object_counter)
							.setOffset(chunk.getKey())
							.setEof((!it.hasNext()) ? true : false).build();

					SimbaMessage objmsg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
							.setObjectFragment(f).setToken(token).setSeq(tid)
							.build();

					server.send(socket, objmsg.toByteString().toByteArray());

				}

				conflict_object_counter++;
			}

			// mark transaction completed
			this.completed = true;

			return;
		}
	}

	public boolean isComplete() {
		return this.completed;
	}

	public void cancel() {
		Iterator<Entry<String, SyncRow>> it = rows.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SyncRow> e = it.next();

			e.getValue().cancel();
		}

	}
}
