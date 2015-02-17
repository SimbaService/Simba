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
import java.util.LinkedList;
import java.util.List;

import android.content.Context;
import android.os.Handler;
import android.util.Log;

import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.SimbaMultiMessage;

/**
 * TODO: Reorganize how these objects access each other.
 * 
 * @author aranya
 * 
 */
public class SimbaNetworkManager {
	private static final String TAG = "SimbaNetworkManager";

	private NetworkIOHandler network = null;
	public TokenManager tokenManager;
	private SimbaMessageHandler worker;
	private List<SimbaMessage.Builder> pendingMessages = new LinkedList<SimbaMessage.Builder>();

	public SimbaNetworkManager(SimbaContentService scs, Context c,
			String host, int port, Handler handler, ConnectionState cs) {
		tokenManager = new TokenManager(this);
		worker = new SimbaMessageHandler(this, scs, tokenManager, handler, cs);

		try {
			network = new NetworkIOHandler(c, worker, handler, host, port, cs);
			new Thread(worker).start();
			new Thread(network).start();

		} catch (IOException e) {
			Log.v(TAG, "Could not initialize network I/O handler");
		}

	}

	public void updateNetworkSettings(String host, int port) {
		network.updateNetworkSettings(host, port);
	}

	public void sendFirstMessage(SimbaMessage m) {

		Log.v(TAG, "Sending first");
		// SimbaLogger.log("u," + m.getSeq() + "," + m.getSeq() + "," +
		// m.computeSize());
		network.sendFirst(m);

	}

	public void sendMessage(SimbaMessage m) {
		network.send(m);
	}

	public void sendMultiMessage(SimbaMultiMessage mm) {
		network.send(mm);
	}

	/*
	 * public void send(byte[] data) { //SimbaLogger.log("u," +
	 * (data.length+4)); network.send(data); }
	 */
	public void sendTokenedMessage(SimbaMessage.Builder b) {
		if (tokenManager.getToken() != null)
			sendTokenedMessageNow(b);
		else {
			this.pendingMessages.add(b);
		}
	}

	public void processPendingMessages() {
		while (!pendingMessages.isEmpty()) {
			SimbaMessage.Builder b = pendingMessages.get(0);
			pendingMessages.remove(0);
			sendTokenedMessageNow(b);
		}
	}

	private void sendTokenedMessageNow(SimbaMessage.Builder b) {
		int seq = SeqNumManager.getSeq();
		SimbaMessage m = b.setToken(tokenManager.getToken()).setSeq(seq)
				.build();

		SeqNumManager.addPendingSeq(seq, m);
		network.send(m);
	}

	public void sendMessageNow(SimbaMessage.Builder b, int seq) {
		SimbaMessage m = b.setToken(tokenManager.getToken()).build();
		network.send(m);
	}

	public void setSyncScheduler(SyncScheduler scheduler) {
		worker.setSyncScheduler(scheduler);
	}

	/*
	 * public void sendWithoutLength(byte[] data) { //SimbaLogger.log("u," +
	 * data.length); network.sendWithoutLength(data); }
	 */
}
