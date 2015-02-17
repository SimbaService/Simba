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
package com.necla.simba.client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.client.ControlListener;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncResponse;
import com.necla.simba.protocol.Client.ActivePullResponse;
import com.necla.simba.protocol.Client.BitmapNotify;
import com.necla.simba.protocol.Client.ControlResponse;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.util.Utils;

public class SimbaMessageHandler implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SimbaMessageHandler.class);
	private List<ClientMessage> queue;
	private ControlListener listener;
	private LinkedBlockingQueue requestQueue;
	private SeqNumManager sequencer;
	private String token;

	public SimbaMessageHandler(ControlListener listener, SeqNumManager sequencer, LinkedBlockingQueue rq) {
		this.listener = listener;
		
		this.sequencer = sequencer;
		this.queue = new LinkedList<ClientMessage>();
		this.requestQueue = rq;
	}

	// Process sync response returned by server
	public void process(byte[] buf) {
		synchronized (queue) {
			try {
				ClientMultiMessage mm = ClientMultiMessage.parseFrom(buf);
				List<ClientMessage> msgs = mm.getMessagesList();

				for (ClientMessage msg: msgs) {
					LOG.debug("Received: seq=" +msg.getSeq() + " type="+ msg.getType());
					
					queue.add(msg);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			queue.notify();
		}
	}
	
	private void process_sync_response(String tbl_id, SyncResponse sr) {

		LOG.debug("Got sync response: " + Utils.stringify(sr));

		List<DataRow> syncedRows = sr.getSyncedRowsList();
		for(DataRow row : syncedRows){
			System.out.println("Synced row: " + row.getId());
		}
		
		List<DataRow> conflictedRows = sr.getConflictedRowsList();
		for(DataRow row : conflictedRows){
			System.out.println("Conflicted row: " + row.getId());
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public void run() {
		while (true) {
			ClientMessage mmsg;

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
			
			final int type = mmsg.getType().getNumber();
			String token;
			
			if (type == ClientMessage.Type.CONTROL_RESPONSE_VALUE) {
				
				ControlResponse crsp = mmsg.getControlResponse();
				ClientMessage pendingMsg = sequencer.getPendingMsg(mmsg.getSeq());
				
				int pendingType;
				if(pendingMsg != null){
					pendingType = pendingMsg.getType().getNumber();
				} else {
					LOG.debug("pendingMsg == null");
					continue;
				}
				if (pendingType == ClientMessage.Type.REG_DEV_VALUE) {
					if (crsp.getStatus()) {
						// store token for later use
						token = crsp.getMsg();
						listener.registrationDone(token);

					} else
						// TODO: authentication failed, notify user
						LOG.debug(crsp.getMsg());
					
				} else if (pendingType == ClientMessage.Type.RECONN_VALUE) {
					if (!crsp.getStatus()) {
						LOG.debug("Reconnect failed, retrying authentication");
						listener.redoRegistration();
					}
					
				} else if (pendingType == ClientMessage.Type.CREATE_TABLE_VALUE) {
					listener.tableCreated();
					
				} else if (!crsp.getStatus()) {

					// Some other failure for some other pending message
					// just log it for now, in the future we might need to process this
					LOG.debug("Operation " 
							+ pendingMsg.getType()
							+ " failed : " + crsp.getMsg());
				}
				sequencer.removePendingSeq(mmsg.getSeq());
			} else if (type == ClientMessage.Type.PULL_DATA_VALUE) {
				SyncHeader s = mmsg.getPullData().getData();
				
				//TODO: THIS IS ONLY FOR THROUGHPUT TESTING. NEED TO DROP
		    	try {
		    		LOG.debug("PUT " + mmsg.getSeq() + " to requestQueue");
		    		if(requestQueue != null){
		    			requestQueue.put(mmsg);
		    		}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				sequencer.removePendingSeq(mmsg.getSeq());
			} else if (type == ClientMessage.Type.SYNC_RESPONSE_VALUE) {
				SyncResponse s = mmsg.getSyncResponse();

				//TODO: THIS IS ONLY FOR THROUGHPUT TESTING. NEED TO DROP
		    	try {
		    		LOG.debug("PUT " + mmsg.getSeq() + " to requestQueue");
//		    		LOG.error("RECEIVED: " + mmsg.getSyncResponse().getSyncedRows(0).getId());
					requestQueue.put(mmsg.getSeq());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				ClientMessage pendingMsg = sequencer.getPendingMsg(mmsg.getSeq());
				assert pendingMsg.getType() == ClientMessage.Type.SYNC_REQUEST;
//				SyncData d = pendingMsg.getSyncResponse();
				sequencer.removePendingSeq(mmsg.getSeq());
				process_sync_response(s.getTbl(), s);
			} else if (type == ClientMessage.Type.BITMAP_NOTIFY_VALUE) {
				BitmapNotify n = mmsg.getBitmapNotify();
//				process_sync_notification(n, mmsg.getSeq());
				LOG.debug("Received Bitmap Notification:");
				LOG.debug(n.getBitmap().toString());
				sequencer.removePendingSeq(mmsg.getSeq());
				try {
		    		LOG.debug("PUT " + mmsg.getSeq() + " to requestQueue");
		    		if(requestQueue != null){
		    			requestQueue.put(mmsg);
		    		}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (type == ClientMessage.Type.ACTIVE_PULL_RESPONSE_VALUE) {
                ActivePullResponse apr = mmsg.getActivePullResponse();
				ClientMessage pendingMsg = sequencer.getPendingMsg(mmsg.getSeq());
				assert pendingMsg.getType() == ClientMessage.Type.ACTIVE_PULL;
				sequencer.removePendingSeq(mmsg.getSeq());
//                process_sync_data_single(apr.getData());
            } else if (type == ClientMessage.Type.OBJECT_FRAGMENT_VALUE){
            	try {
		    		LOG.debug("PUT " + mmsg.getObjectFragment().getTransId() + " to requestQueue");
		    		if(requestQueue != null){
		    			requestQueue.put(mmsg);
		    		}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	handleObjectFragment(mmsg.getObjectFragment());
            } else if (type == ClientMessage.Type.SUB_RESPONSE_VALUE){
		    	try {
		    		LOG.debug("PUT " + mmsg.getSeq() + " to requestQueue");
		    		if(requestQueue != null){
		    			requestQueue.put(mmsg);
		    		}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }

			
		}
	}

	/**
	 * @param objectFragment
	 */
	private void handleObjectFragment(ObjectFragment objectFragment) {
		LOG.debug("oid=" + objectFragment.getOid() + ", offset= "+ objectFragment.getOffset() + ", eof?=" + objectFragment.getEof());
	}

	

	private void check_seq(ClientMessage mmsg) {
		// Make sure the response corresponds to some of our sent messages
		ClientMessage pending_msg = sequencer.getPendingMsg(mmsg.getSeq());
		LOG.debug("Got seq: " + mmsg.getSeq());

		assert (pending_msg != null) : "Received message with invalid seq num...";
		LOG.debug("pending_msg.seq: " + pending_msg.getSeq());
	}
}
