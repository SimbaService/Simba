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
package com.necla.simba.server.gateway.client.sync;


import java.io.IOException;
import java.sql.Date;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.server.gateway.client.ClientState;

/***
 * This class schedules the sync activity: data marshaling, compression, and
 * network transfer. This copy is based on client version of SyncScheduler.
 */
public class SyncScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(SyncScheduler.class.getName());
	
	
	private class SyncItem  {
		
		public SyncItem(long deadline, ClientMessage m) {
			this.deadline = deadline;
			this.m = m;
		}

		public long getDeadline() {
			return deadline;
		}
		
		public ClientMessage getMessage() {
			return this.m;
		}
		
		private final long deadline;
		private final ClientMessage m;
		
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
				ClientMultiMessage.Builder mm = ClientMultiMessage.newBuilder();

				synchronized (pending) {
					int totalSize = 0;

					first = pending.poll();
					assert first != null : "Timer not cancelled even though there is no pending job";
					//LOG.debug(" Picking up to send: " + first.getData());
					// TODO: bos should be a compressed output stream
					totalSize += addMessageForSending(mm, first.getMessage());
					SyncItem next = pending.poll();
					while (next != null && totalSize < MAX_SIZE) {
						//LOG.debug(" Piggyback to send: " + next.getData());
						totalSize += addMessageForSending(mm, next.getMessage());

						if (totalSize >= MAX_SIZE)
							break;
						next = pending.poll();
					}

					next = pending.peek();
					if (next != null) {
						nextTask = new SyncPushTask();
					
						timer.schedule(nextTask, new Date(next.getDeadline()));
					} else
						nextTask = null;
				}

				cs.sendMultiMessage(mm.build());
			} catch (IOException e) {
				LOG.error("Error while building sync data " + e);
			}


		}
	};
	
	

	private final Comparator<SyncItem> COMPARE = new SyncItemComparator();
	private PriorityQueue<SyncItem> pending = new PriorityQueue<SyncItem>(10, COMPARE);
	// Timer for "Time of Decision" (ToD)
	private Timer timer = new Timer();
    private ClientState cs;
	private static int MAX_SIZE = 1024 * 1024;
	private SyncPushTask nextTask;

	
	public SyncScheduler(ClientState cs) {
                this.cs = cs;
	}
	
	public void schedule(ClientMessage m, int syncDT) {
		
		if (syncDT <= 0) {
			// send now!
			sendData(m);
			return;
		}
		
		long now = System.currentTimeMillis();
		long todForNew = now + syncDT;
		synchronized (pending) {
			SyncItem first = pending.peek();
			if (first != null) {
				long dl = first.getDeadline();
				pending.add(new SyncItem(todForNew, m));
				if (todForNew < dl) {
					if (nextTask != null) {
						nextTask.cancel();
					}
					//LOG.debug("Scheduling earlier data");
					nextTask = new SyncPushTask();
					timer.schedule(nextTask,  new Date(todForNew));
				
				
				}
			} else {
				//LOG.debug("Scheduling data dt=" + syncDT);
				pending.add(new SyncItem(todForNew, m));
				nextTask = new SyncPushTask();
				timer.schedule(nextTask, new Date(todForNew));
			}
		}
		
		
		
	}

	/*
	private ByteArrayOutputStream startNewBuffer() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(0);
		bos.write(b.array());
		return bos;
	}
	*/
	
	
	private int addMessageForSending(ClientMultiMessage.Builder mm, ClientMessage m) {
		
		
//		int sz = m.computeSize();
		int sz = m.getSerializedSize();
		mm.addMessages(m);
		//LOG.debug("added message for sending: seq=" + m.getSeq() + " type=" + ClientMessage.Type.getStringValue(m.getType()));
		return sz;
	}
	
	
	
	private void sendData(ClientMessage m) {
		try {
			cs.sendMessage(m);
		} catch (IOException e) {
			LOG.error("Exception while sending data");
		}
	}
}
