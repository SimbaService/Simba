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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.SimbaMultiMessage;
import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.SyncRequest;

import android.util.Log;
import android.util.SparseArray;

/***
 * This class schedules the sync activity: data marshaling, compression, and
 * network transfer.
 */
public class SyncScheduler {
	private static String TAG = "SyncScheduler";

	private class SyncItem {

		public SyncItem(long deadline, SimbaMessage.Builder mb,
				boolean assignSeq) {
			this.deadline = deadline;
			this.assignSeq = assignSeq;
			this.mb = mb;
		}

		public long getDeadline() {
			// TODO Auto-generated method stub
			return deadline;
		}

		public boolean assignSeq() {
			return this.assignSeq;
		}

		public SimbaMessage.Builder getBuilder() {
			return this.mb;
		}

		private final long deadline;
		private final boolean assignSeq;
		private SimbaMessage.Builder mb;

	}

	private class SyncItemComparator implements Comparator<SyncItem> {

		@Override
		public int compare(SyncItem a, SyncItem b) {
			long da = a.getDeadline();
			long db = b.getDeadline();

			if (da - db == 0) {
				return 1;
			} else {
				return (int) (da - db);
			}
		}
	}

	private class SyncPushTask extends TimerTask {
		@Override
		public void run() {
			try {
				SyncItem first;
				ByteArrayOutputStream bos;
				SimbaMultiMessage.Builder mm = SimbaMultiMessage.newBuilder();
				int totalSize = 0;

				synchronized (pending) {

					first = pending.poll();
					assert first != null : "Timer not cancelled even though there is no pending job";
					// Log.v(TAG, " Picking up to send: " + first.getData());
					// TODO: bos should be a compressed output stream
					totalSize += addMessageForSending(mm, first.assignSeq(),
							first.getBuilder());
					SyncItem next = pending.poll();
					while (next != null) {
						// Log.v(TAG, " Piggyback to send: " + next.getData());
						totalSize += addMessageForSending(mm, next.assignSeq(),
								next.getBuilder());
						Log.v(TAG, " Piggyback send with Total size: "
								+ totalSize);
						SimbaLogger.log(SimbaLogger.Type.PIG, SimbaLogger.Dir.UP, totalSize, "piggyback");
						if (totalSize >= Preferences.PIGGYBACK_MAX_SIZE)
							break;

						next = pending.poll();
					}

					next = pending.peek();
					if (next != null) {
						nextTask = new SyncPushTask();
						timer.schedule(nextTask, new Date(next.getDeadline()));
					} else {
						nextTask = null;
					}

				}

				nm.sendMultiMessage(mm.build());

			} catch (IOException e) {
				Log.e(TAG, "Exception while sending data: " + e);
			}
		}
	};

	private final Comparator<SyncItem> COMPARE = new SyncItemComparator();
	private PriorityQueue<SyncItem> pending = new PriorityQueue<SyncItem>(10,
			COMPARE);
	// Timer for "Time of Decision" (ToD)
	private Timer timer = new Timer();
	SimbaNetworkManager nm;
	private ConnectionState cs;
	private SyncPushTask nextTask;

	public SyncScheduler(SimbaNetworkManager manager, ConnectionState cs) {
		this.nm = manager;
		this.nm.setSyncScheduler(this);
		// not used right now
		this.cs = cs;
	}

	public boolean isAuthenticated() {
		return this.nm.tokenManager.getToken() != null;
	}
	
	private void scheduleWithoutDelay(SimbaMessage.Builder mb, boolean assignSeq) {
		boolean queued = false;
		synchronized (pending) {
			if (!pending.isEmpty()) {
				queued = true;
				long now = System.currentTimeMillis();
				pending.add(new SyncItem(now, mb, assignSeq));
				if (nextTask != null)
					nextTask.cancel();
				nextTask = new SyncPushTask();
				timer.schedule(nextTask, 0);
			}
		}
		
		if (!queued)
			sendData(mb, assignSeq);
		
	}

	public void schedule(SimbaMessage.Builder mb, boolean assignSeq, int syncDT) {
		if (syncDT <= 0) {
			scheduleWithoutDelay(mb, assignSeq);
			return;
		}

		long now = System.currentTimeMillis();
		long todForNew = now + syncDT;
		synchronized (pending) {
			SyncItem first = pending.peek();
			if (first != null) {
				long dl = first.getDeadline();
				pending.add(new SyncItem(todForNew, mb, assignSeq));
				if (todForNew < dl) {
					if (nextTask != null) {
						nextTask.cancel();
					}
					Log.v(TAG, "Scheduling earlier data");
					nextTask = new SyncPushTask();
					timer.schedule(nextTask, new Date(todForNew));

				}
			} else {
				Log.v(TAG, "Scheduling data dt=" + syncDT);
				pending.add(new SyncItem(todForNew, mb, assignSeq));
				nextTask = new SyncPushTask();

				timer.schedule(nextTask, new Date(todForNew));
			}
		}
	}

	/*
	 * String token = SimbaMessage mmsg = SimbaMessage.newBuilder()
	 * .setSeq(seq) .setToken(token) .setType(SimbaMessage.Type.SYNC_REQUEST)
	 * .setSyncRequest(r).build();
	 */

	private int addMessageForSending(SimbaMultiMessage.Builder mm,
			boolean assignSeq, SimbaMessage.Builder mb) throws IOException {
		int seq = 0;
		if (assignSeq) {
			seq = SeqNumManager.getSeq();
			mb.setSeq(seq);
		}
		mb.setToken(nm.tokenManager.getToken());
		SimbaMessage mmsg = mb.build();

		
		Log.v(TAG, "added message for sending: seq=" + mmsg.getSeq() + " type="
				+ SimbaMessage.Type.getStringValue(mmsg.getType()));
		
		if (assignSeq)
			SeqNumManager.addPendingSeq(seq, mmsg);
		int sz = mmsg.computeSize();
		if (mmsg.getType() == SimbaMessage.Type.SYNC_REQUEST)
			SimbaLogger.log(SimbaLogger.Type.SYNC_SEND, SimbaLogger.Dir.UP, sz, mmsg.getSyncRequest().getData().getApp() + "." + mmsg.getSyncRequest().getData().getTbl());

		mm.addElementMessages(mmsg);
		
		//SimbaLogger.log(SimbaLogger.Type.BYTES,SimbaLogger.Dir.UP, sz, "addMessage,IGNORE," + mm.hashCode());

		return sz;

	}

	private void sendData(SimbaMessage.Builder mb, boolean assignSeq) {
		int seq = 0;
		if (assignSeq) {
			seq = SeqNumManager.getSeq();
			mb.setSeq(seq);
		}
		mb.setToken(nm.tokenManager.getToken());
		SimbaMessage mmsg = mb.build();
		if (assignSeq)
			SeqNumManager.addPendingSeq(seq, mmsg);
		if (mmsg.getType() == SimbaMessage.Type.SYNC_REQUEST)
			SimbaLogger.log(SimbaLogger.Type.SYNC_SEND, SimbaLogger.Dir.UP, mmsg.computeSize(), mmsg.getSyncRequest().getData().getApp() + "." + mmsg.getSyncRequest().getData().getTbl());
		nm.sendMessage(mmsg);
	}

}
