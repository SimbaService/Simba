/*******************************************************************************
 *    Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
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
package com.necla.simba.client;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.Vector;

import net.jarlehansen.protobuf.javame.ByteString;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.ReadOptions;

import com.necla.simba.client.SimbaLevelDB;
import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.ObjectFragment;
import com.necla.simba.protocol.PullData;
import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.SyncRequest;
import com.necla.simba.protocol.TornRowRequest;

import android.content.Context;
import android.database.Cursor;
import android.util.Log;

/***
 * This class periodically fires the timer to trigger data sync.
 */
public class WriteTimerTask extends TimerTask {
	private final String TAG = "WriteTimerTask";
	private final int period;
	private final SyncScheduler syncScheduler;
	private List<SimbaTable> mtbls = new ArrayList<SimbaTable>();
	private ConnectionState cs;

	/*
	 * TODO: Reorganize this so that we don't need a parent. Each TimerTask
	 * should maintain its own list of tables
	 */
	public WriteTimerTask(int period, SyncScheduler syncScheduler,
			ConnectionState cs) {
		this.period = period;
		this.syncScheduler = syncScheduler;
		this.cs = cs;

	}

	public synchronized void addTable(SimbaTable mtbl) {
		mtbls.add(mtbl);
	}

	public synchronized boolean removeTable(SimbaTable mtbl) {
		mtbls.remove(mtbl);
		return mtbls.size() == 0;
	}

	public void run() {

		// Log.i(TAG, "fired: " + mtbls);

		if (SimbaContentService.isNetworkConnected()) {
			ConnState currentConnState = cs.getConnectionState();

			synchronized (this) {

				for (SimbaTable mtbl : mtbls) {
					// for every table get n/w sync preference and compare with
					// getConnectionState()
					ConnState connPref = mtbl.getSyncNWpref(true);
					long log_syncsize = 0;
					// Log.d(TAG, "Table " + mtbl.getTblId() + " sync pref: " +
					// connPref.toString() + " current: " +
					// currentConnState.toString());

					// wifi > 4g > 3g
					if (currentConnState.getValue() >= connPref.getValue()
							&& syncScheduler.isAuthenticated()
							&& !mtbl.isRecovering()) {

						TornRowRequest trr = mtbl.buildDataForRecover();
						if (!trr.getId().isEmpty()) {
							int seq = SeqNumManager.getSeq();
							SimbaMessage.Builder mb = SimbaMessage
									.newBuilder()
									.setType(SimbaMessage.Type.TORN_REQUEST)
									.setSeq(seq).setTornRowRequest(trr);
							syncScheduler.schedule(mb, false,
									mtbl.getSyncDT(true));
							SeqNumManager.addPendingSeq(seq, mb.build());
							Log.d(TAG,
									"Sending TORN_REQUEST! app: "
											+ trr.getApp() + ", tbl: "
											+ trr.getTbl() + ", seq: " + seq);
							SimbaLogger.log(SimbaLogger.Type.FAIL, SimbaLogger.Dir.UP,0,  
									"tr," + 1); // for now just a count of Torn requests

							continue;
						}

						Map<Integer, Long> obj_list = new HashMap<Integer, Long>();

						// take snapshot before creating SyncRequest message
						ReadOptions ro = SimbaLevelDB.takeSnapshot();

						SyncHeader h = mtbl.buildDataForSyncing(obj_list);
						if (h.getDirtyRows().isEmpty()
								&& h.getDeletedRows().isEmpty()) {
							// Log.i(TAG, mtbl.toString() +
							// ": no change (dirty/deleted)");

						} else {
							SyncRequest r = SyncRequest.newBuilder().setData(h)
									.build();
							SimbaMessage.Builder mb = SimbaMessage
									.newBuilder()
									.setType(SimbaMessage.Type.SYNC_REQUEST)
									.setSeq(r.getData().getTrans_id())
									.setSyncRequest(r);
						SimbaLogger.log(SimbaLogger.Type.SCHED, SimbaLogger.Dir.UP, mtbl.getSyncDT(true), "SYNC: " + mtbl.getAppId()+"." + mtbl.getTblId());

							syncScheduler.schedule(mb, false,
									mtbl.getSyncDT(true));
							SeqNumManager.addPendingSeq(r.getData()
									.getTrans_id(), mb.build());

							Log.d(TAG, "Sending SYNC_REQUEST! app: "
									+ r.getData().getApp() + ", tbl: "
									+ r.getData().getTbl() + ", trans_id: "
									+ r.getData().getTrans_id());

							/* create ObjectFragments */
							for (Map.Entry<Integer, Long> entry : obj_list
									.entrySet()) {
								int oid = entry.getKey();
								long objID = entry.getValue();

								// if dirty chunk list exists, send only those
								// chunks
								if (SimbaChunkList.getDirtyChunks(objID) != null) {
									BitSet dirtyChunkList = SimbaChunkList
											.getDirtyChunks(objID);
									for (int i = dirtyChunkList.nextSetBit(0); i >= 0;) {
										int chunk_num = i;
										byte[] buffer = SimbaLevelDB.getChunk(
												ro, objID, chunk_num);
										i = dirtyChunkList.nextSetBit(i + 1);
										
										ObjectFragment of = ObjectFragment
												.newBuilder()
												.setTrans_id(
														r.getData()
																.getTrans_id())
												.setOid(oid)
												.setOffset(chunk_num)
												.setData(
														ByteString
																.copyFrom(buffer))
												.setEof(i == -1 ? true : false)
												.build();

										Log.d(TAG,
												"Sending OBJECT_FRAGMENT! trans_id: "
														+ of.getTrans_id()
														+ ", oid: "
														+ of.getOid()
														+ ", offset: "
														+ of.getOffset());

										SimbaMessage.Builder mb_of = SimbaMessage
												.newBuilder()
												.setType(
														SimbaMessage.Type.OBJECT_FRAGMENT)
												.setSeq(r.getData()
														.getTrans_id())
												.setObjectFragment(of);
										syncScheduler.schedule(mb_of, false,
												mtbl.getSyncDT(true));
									}
								}
								// if dirty chunk list does not exist, send
								// entire object
								else {
									int numChunks = SimbaLevelDB
											.getNumChunks(objID);
									for (int i = 0; i < numChunks; i++) {
										byte[] buffer = SimbaLevelDB.getChunk(
												ro, objID, i);
										ObjectFragment of = ObjectFragment
												.newBuilder()
												.setTrans_id(
														r.getData()
																.getTrans_id())
												.setOid(oid)
												.setOffset(i)
												.setData(
														ByteString
																.copyFrom(buffer))
												.setEof(i + 1 == numChunks ? true
														: false).build();

										Log.d(TAG,
												"Sending all OBJECT_FRAGMENTs! trans_id: "
														+ of.getTrans_id()
														+ ", oid: "
														+ of.getOid()
														+ ", offset: "
														+ of.getOffset());

										SimbaMessage.Builder mb_of = SimbaMessage
												.newBuilder()
												.setType(
														SimbaMessage.Type.OBJECT_FRAGMENT)
												.setSeq(r.getData()
														.getTrans_id())
												.setObjectFragment(of);
										syncScheduler.schedule(mb_of, false,
												mtbl.getSyncDT(true));
									}
								}
							}
						
							// for every table that is dirty, measure data
							Log.d(TAG, "Size of sync request for table : " + mtbl.getAppId() + ":"+mtbl.getTblId() +  " Size: " + r.computeSize());
							log_syncsize += r.computeSize();			
						}
						if(log_syncsize > 0)
							SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.UP, log_syncsize, "su");
						
						// close snapshot after all sync operations are done
						SimbaLevelDB.closeSnapshot(ro);
					}
				}
			}

		}
	}
}
