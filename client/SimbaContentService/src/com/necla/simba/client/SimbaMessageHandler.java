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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import net.jarlehansen.protobuf.javame.ByteString;

import android.app.Notification;
import android.content.ContentValues;
import android.database.sqlite.SQLiteDatabase;
import android.os.Handler;
import android.os.Message;
import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.protocol.ActivePullResponse;
import com.necla.simba.protocol.BitmapNotify;
import com.necla.simba.protocol.Column;
import com.necla.simba.protocol.ControlResponse;
import com.necla.simba.protocol.DataRow;
import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.SimbaMultiMessage;
import com.necla.simba.protocol.NotificationPull;
import com.necla.simba.protocol.Notify;
import com.necla.simba.protocol.ObjectFragment;
import com.necla.simba.protocol.ObjectHeader;
import com.necla.simba.protocol.PassivePull;
import com.necla.simba.protocol.PullData;
import com.necla.simba.protocol.RegisterDevice;
import com.necla.simba.protocol.SubscribeTable;
import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.SyncRequest;
import com.necla.simba.protocol.SyncResponse;
import com.necla.simba.protocol.TornRowResponse;

public class SimbaMessageHandler implements Runnable {
	private String TAG = "SimbaMessageHandler";
	private List<SimbaMessage> queue;
	private SimbaNetworkManager nm;
	private SimbaContentService scs;
	private TokenManager tokenManager;
	private Handler handler;
	private ConnectionState cs;
	private SyncScheduler scheduler;
	Map<Integer, SimbaSyncObject> transactions = new HashMap<Integer, SimbaSyncObject>();

	public SimbaMessageHandler(SimbaNetworkManager manager,
			SimbaContentService scs, TokenManager tokenManager,
			Handler handler, ConnectionState cs) {
		this.queue = new ArrayList<SimbaMessage>();
		this.nm = manager;
		this.scs = scs;
		this.tokenManager = tokenManager;
		this.handler = handler;
		this.cs = cs;
	}

	// Process sync response returned by server
	public void process(byte[] buf) {
		synchronized (queue) {
			try {
				SimbaMultiMessage mm = SimbaMultiMessage.parseFrom(buf);
				List<SimbaMessage> msgs = mm.getMessages();
				// SimbaLogger.log("mm, " + mm.computeSize());
				// SimbaLogger.log("sr, " + buf.length);
				SimbaLogger.log(SimbaLogger.Type.MMMSG, SimbaLogger.Dir.DN,
						buf.length,
						"Decompressed, msgs: " + mm.getMessages().size() + ", "
								+ mm.hashCode());

				for (SimbaMessage m : msgs) {
					Log.d(TAG, "Received: seq=" + m.getSeq() + ", type="
							+ SimbaMessage.Type.getStringValue(m.getType()));
					SimbaLogger.log(
							SimbaLogger.Type.MMSG,
							SimbaLogger.Dir.DN,
							m.computeSize(),
							SimbaMessage.Type.getStringValue(m.getType())
									+ " : " + m.getType() + ", "
									+ mm.hashCode());

					queue.add(m);
				}

			} catch (IOException e) {
				Log.w(TAG, "Could not parse message from network, dropping");
			}
			queue.notify();
		}
	}

	private void process_sync_response_table_not_found(SyncResponse response,
			SimbaTable mtbl) {
		// This sync did not succeed becaue the server did not know
		// about this table. Set the schema sync pending flag for
		// the table. Also, clear the _sync flag and set _dirty for the rows so
		// that they
		// get synced the next time around
		mtbl.setSchemaSyncPending(true);
		Vector<DataRow> ok_rows = response.getSyncedRows();
		for (DataRow row : ok_rows) {
			String id = row.getId();

			ContentValues cv = new ContentValues();
			cv.put("_sync", Boolean.FALSE);

			int affected_rows = mtbl.updateInternal(cv, "_id=?",
					new String[] { id });

			if (affected_rows < 1) {
				cv = new ContentValues();
				/* user might have deleted that row */
				mtbl.updateDelete(cv, "_id=?", new String[] { id });
			}
		}
	}

	private void process_subscribe_response(String app_id, String tbl_id,
			List<Column> columns) {
		// check if table already exists
		if (SimbaTableManager.getTable(app_id, tbl_id) != null) {
			Log.d(TAG, "Table <" + app_id + ", " + tbl_id + "> already created");
		}
		// create table if it does not exists
		else {
			Log.d(TAG, "Table <" + app_id + ", " + tbl_id + "> does not exist");
			String cmd = "";
			int i = 0;
			for (Column c : columns) {
				cmd += c.getName() + " ";
				if (c.getType() == Column.Type.VARCHAR) {
					cmd += "VARCHAR, ";
				} else if (c.getType() == Column.Type.OBJECT) {
					cmd += "BIGINT, ";
				} else if (c.getType() == Column.Type.INT) {
					cmd += "INT, ";
				} else {
					Log.d(TAG, "Wrong type! " + c.getType());
				}
			}
			if (cmd != null) {
				scs.createTable(app_id, tbl_id,
						cmd.substring(0, cmd.length() - 2),
						new TableProperties(false));
				try {
					ISCSClient client = ClientCallbackManager
							.getCallback(app_id);
					if (client != null) {
						// alert app that table has been created
						client.subscribeDone();
					}
				} catch (RemoteException e) {
					notifyDeadClient(app_id);
				}
			}
		}
	}

	private void process_sync_response(String app_id, String tbl_id,
			SyncHeader sh, SyncResponse sr, Map<Integer, Long> obj_list) {

		Log.v(TAG, "Got sync response: " + Utils.stringify(sr));

		// 0. grab Simba table handle

		SimbaTable mtbl = SimbaTableManager.getTable(app_id, tbl_id);
		if (mtbl == null) {
			Log.v(TAG, "Fail to look up SimbaTable for " + app_id + "/"
					+ tbl_id);
			return;
		}

		if (sr.getResult() == SyncResponse.SyncResult.TABLE_NOT_FOUND) {
			process_sync_response_table_not_found(sr, mtbl);

			Log.v(TAG, "Schema sync pending for " + app_id + "/" + tbl_id);
			return;
		}

		List<DataRow> conflictedRows = sr.getConflictedRows();

		// since either column data or object could be conflicted while both of
		// them were requested for sync, we pass SyncHeader's dirtyRows to set
		// dirty flag back accordingly
		int conflictedObjRows = mtbl.processSyncResponse(sh.getDirtyRows(),
				sr.getSyncedRows(), conflictedRows, obj_list);

		// if (!conflictedRows.isEmpty()) {
		if (conflictedRows.size() - conflictedObjRows > 0) {
			// notify user app
			try {
				ISCSClient client = ClientCallbackManager.getCallback(app_id);
				if (client != null) {
					client.syncConflict(tbl_id, conflictedRows.size()
							- conflictedObjRows);
				}
				// ClientCallbackManager.getCallback(app_id).syncConflict(tbl_id,
				// conflictedRows.size());
			} catch (RemoteException e) {
				notifyDeadClient(app_id);
				// e.printStackTrace();
			}
		}
	}

	// Process server's notification message and initiate data pull if needed
	private void process_sync_notification(Notify notify, int seq) {
		Log.v(TAG, "Got notification for read timer: " + notify.getPeriod());
		SimbaLogger.log(SimbaLogger.Type.NOTIFY, SimbaLogger.Dir.DN,
				notify.computeSize(), "");
		ConnState currentConnState = cs.getConnectionState();

		// TODO: we may decouple the passive pull operation

		/*
		 * check for global read sync preference (wifi etc) and pull or not.
		 * Right now its all or nothing across tables If decide not to pull,
		 * send a NOTIFY_ACK msg with seqnum Server should understand msg and
		 * not read data TODO: make it per table
		 */

		// if (currentConnState.getValue() >=
		// SimbaContentService.globalReadSyncPref.getValue()) {
		// if (true) {
		// Log.d(TAG, "Not checking for CONN state in Message Handler");
		if (currentConnState.getValue() >= Preferences.globalReadSyncPref
				.getValue()) {
			PassivePull p = PassivePull.newBuilder()
					.setPeriod(notify.getPeriod()).build();

			SimbaMessage.Builder mmsg = SimbaMessage.newBuilder().setSeq(seq)
					.setToken(nm.tokenManager.getToken())
					.setType(SimbaMessage.Type.PASSIVE_PULL).setPassivePull(p);
			// send actual request
			scheduler.schedule(mmsg, false, Preferences.DT_CONTROL_MSGS_PULL);
		} else {
			// sync pref do not allow read sync, just send ack
			SimbaMessage.Builder mmsg = SimbaMessage.newBuilder().setSeq(seq)
					.setType(SimbaMessage.Type.NOTIFY_ACK);
			scheduler.schedule(mmsg, false, Preferences.DT_CONTROL_MSGS_ACK);
		}
	}

	private void process_bitmap_notify(BitmapNotify bn) {

		BitSet bits = Utils.fromByteArray(bn.getBitmap().toByteArray());

		for (int i = 0; i < bits.size(); i++) {
			if (bits.get(i)) {
				Log.d(TAG, "BITMAP_NOTIFY! bit[" + i + "] is set");

				// send NotificationPull message to server
				String[] uid_tid = SimbaContentService.getUidTid(i).split(
						"\\,");
				SimbaTable mtbl = SimbaTableManager.getTable(uid_tid[0],
						uid_tid[1]);
				Log.d(TAG, "Got table with <" + uid_tid[0] + ", " + uid_tid[1]
						+ ">");

				if (mtbl.getCR()) {
					Log.d(TAG,
							"User is in CR! Setting SendNotificationPull to recover PullData!");
					mtbl.setSendNotificationPull(true);
				} else {
					NotificationPull.Builder np = NotificationPull.newBuilder()
							.setApp(uid_tid[0]).setTbl(uid_tid[1])
							.setFromVersion(mtbl.getRev());
					int seq = SeqNumManager.getSeq();
					SimbaMessage.Builder m = SimbaMessage.newBuilder()
							.setSeq(seq)
							.setType(SimbaMessage.Type.NOTIFICATION_PULL)
							.setNotificationPull(np.build());
					nm.sendTokenedMessage(m);
					SeqNumManager.addPendingSeq(seq, m.build());

					Log.d(TAG, "Sending NOTIFICATION_PULL! table: "
							+ uid_tid[1] + ", fromVersion: " + mtbl.getRev());
				}
			}
		}
	}

	private Map<Integer, Long> process_torn_row_response(SyncHeader sds) {
		Log.v(TAG, "process_torn_row_response " + Utils.stringify(sds));
		// SimbaLogger.log("srm, " + Utils.stringify(sds) + ", " +
		// sds.computeSize());

		// grab Simba table handle
		String app = sds.getApp();
		String tbl = sds.getTbl();
		SimbaTable mtbl = SimbaTableManager.getTable(app, tbl);
		if (mtbl == null) {
			Log.v(TAG, "Fail to look up SimbaTable for " + app + "/" + tbl);
			return null;
		}

		// call processSyncData, which handles TornRowResponse and PullData
		Map<Integer, Long> obj_list = new HashMap<Integer, Long>();
		int[] counts = mtbl.processSyncData(sds.getDirtyRows(),
				sds.getDeletedRows(), obj_list, false);

		// notify app for new data, if any
		// there isn't any conflict row for torn recovery
		if (counts[0] > 0 || counts[2] > 0) {
			try {
				ISCSClient client = ClientCallbackManager.getCallback(app);
				if (client != null) {
					client.newData(tbl, counts[0], counts[2]);
				}
			} catch (RemoteException e) {
				notifyDeadClient(app);
			}
		}

		return obj_list;
	}

	private Map<Integer, Long> process_sync_data_single(SyncHeader sds) {
		Log.v(TAG, "process_sync_data_single " + Utils.stringify(sds));

		// 0. grab Simba table handle
		String app = sds.getApp();
		String tbl = sds.getTbl();
		SimbaTable mtbl = SimbaTableManager.getTable(app, tbl);
		if (mtbl == null) {
			Log.v(TAG, "Fail to look up SimbaTable for " + app + "/" + tbl);
			return null;
		}

		Map<Integer, Long> obj_list = new HashMap<Integer, Long>();
		int[] counts = mtbl.processSyncData(sds.getDirtyRows(),
				sds.getDeletedRows(), obj_list, true);

		// 3. conflict rows, if any
		if (counts[1] > 0) {
			try {
				ISCSClient client = ClientCallbackManager.getCallback(app);
				if (client != null) {
					client.syncConflict(tbl, counts[1]);
				}
			} catch (RemoteException e) {
				notifyDeadClient(app);
				// e.printStackTrace();
			}
		}

		// 4. notify app for new data, if any
		if (counts[0] > 0 || counts[2] > 0) {
			try {
				ISCSClient client = ClientCallbackManager.getCallback(app);
				if (client != null) {
					client.newData(tbl, counts[0], counts[2]);
				}
			} catch (RemoteException e) {
				notifyDeadClient(app);

				// e.printStackTrace();
			}
		}
		return obj_list;
	}

	private void process_object_fragment_eof(DataRow row, SimbaSyncObject mso,
			boolean isSyncResponse) {
		SimbaTable mtbl = SimbaTableManager.getTable(mso.getApp(),
				mso.getTbl());
		int[] counts = mtbl.processObjectFragmentEof(mso, row, isSyncResponse);
		mso.setRowCounts(counts);

		if (mso.remainingObjects() == 0) {
			// conflict rows, if any
			if (mso.getConflictedRows() > 0) {
				// notify user app
				try {
					ISCSClient client = ClientCallbackManager.getCallback(mso
							.getApp());
					if (client != null) {
						client.syncConflict(mso.getTbl(), counts[1]);
					}
				} catch (RemoteException e) {
					notifyDeadClient(mso.getApp());
					// e.printStackTrace();
				}
			}
			// notify app for new data, if any
			if (mso.getNewRows() > 0 || mso.getDeletedRows() > 0) {
				try {
					ISCSClient client = ClientCallbackManager.getCallback(mso
							.getApp());
					if (client != null) {
						client.newData(mso.getTbl(), counts[0], counts[2]);
					}
				} catch (RemoteException e) {
					notifyDeadClient(mso.getApp());
					// e.printStackTrace();
				}
			}
		}
	}

	private void notifyDeadClient(String client) {
		Message m = handler.obtainMessage(InternalMessages.CLIENT_LOST);
		m.obj = client;
		handler.sendMessage(m);
	}

	@SuppressWarnings("unchecked")
	public void run() {
		while (true) {
			SimbaMessage mmsg;

			// Wait for data to become available
			synchronized (queue) {
				while (queue.isEmpty()) {
					try {
						queue.wait();
					} catch (InterruptedException e) {
					}
				}
				mmsg = queue.remove(0);
			}

			final int type = mmsg.getType();

			if (type == SimbaMessage.Type.CONTROL_RESPONSE) {
				ControlResponse crsp = mmsg.getControlResponse();
				SimbaMessage pendingMsg = SeqNumManager.getPendingMsg(mmsg
						.getSeq());

				final int pendingType = pendingMsg.getType();
				SimbaLogger.log(SimbaLogger.Type.CTRL, SimbaLogger.Dir.DN,
						pendingMsg.computeSize(),
						SimbaMessage.Type.getStringValue(pendingType));
				if (pendingType == SimbaMessage.Type.REG_DEV) {
					if (crsp.getStatus()) {
						// store token for later use
						tokenManager.setToken(crsp.getMsg());
						handler.sendEmptyMessage(InternalMessages.AUTHENTICATION_DONE);
					} else
						// TODO: authentication failed, notify user
						Log.e(TAG, crsp.getMsg());

				} else if (pendingType == SimbaMessage.Type.RECONN) {
					if (!crsp.getStatus()) {
						Log.e(TAG, "Reconnect failed, retrying authentication");
						reg_dev();
					}

				} else if (pendingType == SimbaMessage.Type.CREATE_TABLE) {
					if (crsp.getStatus()) {
						Log.d(TAG, "Create table success: " + crsp.getMsg());
					} else {
						Log.e(TAG, "Create table failed: " + crsp.getMsg());
					}
				} else if (!crsp.getStatus()) {
					// Some other failure for some other pending message
					// just log it for now, in the future we might need to
					// process this
					Log.w(TAG,
							"Operation "
									+ SimbaMessage.Type
											.getStringValue(pendingMsg
													.getType()) + " failed : "
									+ crsp.getMsg());
				}
				SeqNumManager.removePendingSeq(mmsg.getSeq());
			} else if (type == SimbaMessage.Type.SUB_RESPONSE) {
				List<Column> c = mmsg.getSubscribeResponse().getColumns();
				SimbaMessage pendingMsg = SeqNumManager.getPendingMsg(mmsg
						.getSeq());
				assert pendingMsg.getType() == SimbaMessage.Type.SUB_TBL;
				SubscribeTable s = pendingMsg.getSubscribeTable();
				// handle subscribe table response
				process_subscribe_response(s.getApp(), s.getTbl(), c);
				SeqNumManager.removePendingSeq(mmsg.getSeq());
			} else if (type == SimbaMessage.Type.TORN_RESPONSE) {
				SyncHeader s = mmsg.getTornRowResponse().getData();
				Map<Integer, Long> obj_list = process_torn_row_response(s);

				// add to transactions list if object exists, else remove
				// pending message
				SimbaSyncObject mso = new SimbaSyncObject(s.getApp(),
						s.getTbl(), s.getDirtyRows(), obj_list,
						SimbaMessage.Type.TORN_REQUEST);
				if (mso.remainingObjects() > 0) {
					Log.d(TAG, "Waiting for " + mso.remainingObjects()
							+ " objects");
					transactions.put(s.getTrans_id(), mso);
				} else {
					// set isRecovering to false
					SimbaTable mtbl = SimbaTableManager.getTable(s.getApp(),
							s.getTbl());
					mtbl.doneRecovering();
				}
				SeqNumManager.removePendingSeq(mmsg.getSeq());

			} else if (type == SimbaMessage.Type.BITMAP_NOTIFY) {
				BitmapNotify bn = mmsg.getBitmapNotify();
				process_bitmap_notify(bn);
			} else if (type == SimbaMessage.Type.PULL_DATA) {
				SyncHeader s = mmsg.getPullData().getData();
				SimbaMessage pendingMsg = SeqNumManager.getPendingMsg(mmsg
						.getSeq());
				Map<Integer, Long> obj_list = process_sync_data_single(s);

				// add to transactions list if object exists, else remove
				// pending message
				SimbaSyncObject mso = new SimbaSyncObject(s.getApp(),
						s.getTbl(), s.getDirtyRows(), obj_list,
						pendingMsg.getType());
				if (mso.remainingObjects() > 0) {
					Log.d(TAG, "Waiting for " + mso.remainingObjects()
							+ " objects");
					transactions.put(s.getTrans_id(), mso);
					Log.d(TAG, "PULL_DATA X ID: " + s.getTrans_id());

				} else {
					SimbaTable mtbl = SimbaTableManager.getTable(s.getApp(),
							s.getTbl());
					if (mtbl.isStopPullData()) {
						mtbl.resetStopPullData();
						if (!mtbl.getCR()) {
							Log.d(TAG,
									"Sending NotificationPull to recover PullData!");

							// send NotificationPull from tbl_rev
							NotificationPull.Builder np_msg = NotificationPull
									.newBuilder().setApp(mtbl.getAppId())
									.setTbl(mtbl.getTblId())
									.setFromVersion(mtbl.getRev());
							int seq = SeqNumManager.getSeq();
							SimbaMessage.Builder m = SimbaMessage
									.newBuilder()
									.setSeq(seq)
									.setType(
											SimbaMessage.Type.NOTIFICATION_PULL)
									.setNotificationPull(np_msg.build());
							nm.sendTokenedMessage(m);
							SeqNumManager.addPendingSeq(seq, m.build());
						} else {
							Log.d(TAG,
									"Setting SendNotificationPull to recover PullData!");
							mtbl.setSendNotificationPull(true);
						}
					}
				}
				SeqNumManager.removePendingSeq(mmsg.getSeq());

			} else if (type == SimbaMessage.Type.SYNC_RESPONSE) {
				SyncResponse s = mmsg.getSyncResponse();
				Map<Integer, Long> obj_list = new HashMap<Integer, Long>();
				SimbaMessage pendingMsg = SeqNumManager.getPendingMsg(mmsg
						.getSeq());
				assert pendingMsg.getType() == SimbaMessage.Type.SYNC_REQUEST;

				SyncHeader d = pendingMsg.getSyncRequest().getData();
				process_sync_response(d.getApp(), d.getTbl(), d, s, obj_list);

				// add to transactions list if object exists
				SimbaSyncObject mso = new SimbaSyncObject(s.getApp(),
						s.getTbl(), s.getConflictedRows(), obj_list,
						pendingMsg.getType());
				if (mso.remainingObjects() > 0) {
					Log.d(TAG, "Waiting for " + mso.remainingObjects()
							+ " objects");
					transactions.put(d.getTrans_id(), mso);
				} else {
					// remove dirty chunk list from the table
					SimbaChunkList
							.removeDirtyChunkList(d.getApp(), d.getTbl());

				}
				SeqNumManager.removePendingSeq(mmsg.getSeq());
			} else if (type == SimbaMessage.Type.NOTIFY) {
				Notify n = mmsg.getNotify();
				process_sync_notification(n, mmsg.getSeq());
			} else if (type == SimbaMessage.Type.ACTIVE_PULL_RESPONSE) {
				ActivePullResponse apr = mmsg.getActivePullResponse();
				SimbaMessage pendingMsg = SeqNumManager.getPendingMsg(mmsg
						.getSeq());
				assert pendingMsg.getType() == SimbaMessage.Type.ACTIVE_PULL;
				SeqNumManager.removePendingSeq(mmsg.getSeq());

				Map<Integer, Long> obj_list = process_sync_data_single(apr
						.getData());
				transactions
						.put(apr.getData().getTrans_id(), new SimbaSyncObject(
								apr.getData().getApp(), apr.getData().getTbl(),
								apr.getData().getDirtyRows(), obj_list,
								pendingMsg.getType()));
			} else if (type == SimbaMessage.Type.OBJECT_FRAGMENT) {

				ObjectFragment of = mmsg.getObjectFragment();
				Log.d(TAG, "OBJ_FRAG X ID: " + of.getTrans_id());
				SimbaSyncObject mso = transactions.get(of.getTrans_id());
				DataRow row = null;
				if (mso != null && (row = mso.addFragment(of)) != null) {
					Log.d(TAG, "Receive Row ID: " + row.getId() + " completed");

					if (mso.getType() == SimbaMessage.Type.SYNC_REQUEST) {
						// handle object fragments from conflictedRows of
						// SyncResponse message
						process_object_fragment_eof(row, mso, true);
					} else if (mso.getType() == SimbaMessage.Type.NOTIFICATION_PULL
							|| mso.getType() == SimbaMessage.Type.TORN_REQUEST) {
						// handle object fragments from dirtyRows of PullData
						// message
						process_object_fragment_eof(row, mso, false);
					}
				} else {
					Log.d(TAG, "Null mso");
				}

				// complete object sync transaction
				if (mso != null && mso.remainingObjects() == 0) {
					// completed duplicate row
					// set tbl_rev to this row's server_rev
					if (row == null) {
						int server_rev = mso.getRowRev(of);
						assert mso.getType() == SimbaMessage.Type.NOTIFICATION_PULL;
						SimbaTable mtbl = SimbaTableManager.getTable(
								mso.getApp(), mso.getTbl());

						// if stopPullData is set, don't update the tbl_rev
						if (!mtbl.isStopPullData()) {
							mtbl.setTblRev(server_rev);
						} else {
							Log.d(TAG, "Not updating tbl_rev!");
							mtbl.resetStopPullData();
							if (!mtbl.getCR()) {
								Log.d(TAG,
										"Sending NotificationPull to recover PullData!");

								// send NotificationPull from tbl_rev
								NotificationPull.Builder np_msg = NotificationPull
										.newBuilder().setApp(mtbl.getAppId())
										.setTbl(mtbl.getTblId())
										.setFromVersion(mtbl.getRev());
								int seq = SeqNumManager.getSeq();
								SimbaMessage.Builder m = SimbaMessage
										.newBuilder()
										.setSeq(seq)
										.setType(
												SimbaMessage.Type.NOTIFICATION_PULL)
										.setNotificationPull(np_msg.build());
								nm.sendTokenedMessage(m);
								SeqNumManager.addPendingSeq(seq, m.build());
							} else {
								Log.d(TAG,
										"Setting SendNotificationPull to recover PullData!");
								mtbl.setSendNotificationPull(true);
							}
						}
					}
					SeqNumManager.removePendingSeq(mmsg.getSeq());
				}
			}
		}
	}

	private void reg_dev() {
		int seq = SeqNumManager.getSeq();
		RegisterDevice r = RegisterDevice.newBuilder()
				.setDeviceId(WalletManager.getDeviceID())
				.setUserId(WalletManager.getUserID())
				.setPassword(WalletManager.getUserPassword()).build();

		SimbaMessage m = SimbaMessage.newBuilder().setSeq(seq)
				.setType(SimbaMessage.Type.REG_DEV).setRegisterDevice(r)
				.build();

		SeqNumManager.addPendingSeq(seq, m);
		nm.sendMessage(m);
	}

	private void check_seq(SimbaMessage mmsg) {
		// Make sure the response corresponds to some of our sent messages
		SimbaMessage pending_msg = SeqNumManager.getPendingMsg(mmsg.getSeq());
		Log.v(TAG, "Got seq: " + mmsg.getSeq());

		assert (pending_msg != null) : "Received message with invalid seq num...";
		Log.v(TAG, "pending_msg.seq: " + pending_msg.getSeq());
	}

	public void setSyncScheduler(SyncScheduler scheduler) {
		this.scheduler = scheduler;

	}
}
