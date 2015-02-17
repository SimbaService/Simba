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
import com.necla.simba.client.SimbaMessageHandler;
import com.necla.simba.client.NetworkIOHandler;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Common.CreateTable;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.protocol.Client.RegisterDevice;

/**
 * TODO: Reorganize how these objects access each other.
 * @author aranya
 *
 */
public class SimbaNetworkManager implements ControlListener {
	private static final Logger LOG = LoggerFactory.getLogger(SimbaNetworkManager.class);
	SeqNumManager sequencer = new SeqNumManager();

	private NetworkIOHandler network = null;
	private SimbaMessageHandler worker;
	private List<ClientMessage.Builder> pendingMessages = new LinkedList<ClientMessage.Builder>();
	private String deviceId;
	private String token;
	private final Object o = new Object();
	
	
	public SimbaNetworkManager(String host, int port, SeqNumManager sequencer, LinkedBlockingQueue requestQueue, int clientid) {
		this.sequencer = sequencer;
		worker = new SimbaMessageHandler(this, sequencer, requestQueue);
		
		try {
		network = new NetworkIOHandler(worker,host, port, clientid);
		new Thread(worker).start();
		new Thread(network).start();
		
		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("Could not initialize network I/O handler");
		}
		
	}
	
	public SimbaNetworkManager(String host, int port, SeqNumManager sequencer, LinkedBlockingQueue requestQueue) {
		this.sequencer = sequencer;
		worker = new SimbaMessageHandler(this, sequencer, requestQueue);
		
		try {
		network = new NetworkIOHandler(worker,host, port, 0);
		new Thread(worker).start();
		new Thread(network).start();
		
		} catch (IOException e) {
			e.printStackTrace();
			LOG.debug("Could not initialize network I/O handler");
		}
		
	}
	
	public NetworkIOHandler getNetwork() {
		return network;
	}
	
	public void updateNetworkSettings(String host, int port)
	{
		network.updateNetworkSettings(host,  port);
	}
	
	public void sendFirstMessage(ClientMessage m) {


		LOG.debug("Sending firs type=" + m.getType() + " seq=" + m.getSeq() + " sz=" + m.getSerializedSize());
		network.sendFirst(m);

	}
	
	public void sendMessage(ClientMessage m) {
		LOG.debug("Sending type=" + m.getType() + " seq=" + m.getSeq() + " sz=" + m.getSerializedSize());
		network.send(m);
	}
	
	public void sendMultiMessage(ClientMultiMessage mm) {
		network.send(mm);
	}

	public void sendTokenedMessage(ClientMessage.Builder b) {		
		if (new String("token") != null)
			sendTokenedMessageNow(b);
		else {
			this.pendingMessages.add(b);
		}
	}
	
	public void processPendingMessages() {
		while (!pendingMessages.isEmpty()) {
			ClientMessage.Builder b = pendingMessages.get(0);
			pendingMessages.remove(0);
			sendTokenedMessageNow(b);
		}
	}
	
	private void sendTokenedMessageNow(ClientMessage.Builder b) {
		int seq = sequencer.getSeq();
		ClientMessage m = b.setToken("token")
			.setSeq(seq).build();
		
		sequencer.addPendingSeq(seq, m);
		network.send(m);
	}
	
	public void registerDevice(String deviceId) {
		this.deviceId = deviceId;
		doRegister();
	}
	
	public void createTable(CreateTable ct) {
		int seq = sequencer.getSeq();
		ClientMessage msg = ClientMessage.newBuilder()
				.setType(ClientMessage.Type.CREATE_TABLE)
				.setCreateTable(ct).setToken(token)
				.setSeq(seq).build();

		sequencer.addPendingSeq(seq, msg);
		synchronized (o) {
			network.send(msg);
			try {
				o.wait();
			} catch (InterruptedException e) {

			}
		}

	}
	
	private void doRegister() {
		int seq = sequencer.getSeq();

		RegisterDevice rd = RegisterDevice.newBuilder()
				.setDeviceId(this.deviceId).setUserId("user")
				.setPassword("pass").build();

		ClientMessage m1 = ClientMessage.newBuilder()
				.setType(ClientMessage.Type.REG_DEV).setRegisterDevice(rd)
				.setSeq(seq).build();

		sequencer.addPendingSeq(seq, m1);
		
		synchronized (o) {

			sendMessage(m1);
			try {
				o.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void registrationDone(String token) {
		synchronized (o) {
			this.token = token;
			o.notify();
		}


	}
	@Override
	public void redoRegistration() {
		doRegister();
		
	}
	@Override
	public void tableCreated() {
		synchronized (o) {
			o.notify();
		}
		
	}
}
