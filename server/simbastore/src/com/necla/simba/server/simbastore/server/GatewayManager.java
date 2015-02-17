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
package com.necla.simba.server.simbastore.server;

import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.protocol.Common.AbortTransaction;
import com.necla.simba.protocol.Server.Ping;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.SeqNumManager;
import com.necla.simba.server.simbastore.server.data.ServerDataEvent;

public class GatewayManager {

	private static final Logger LOG = LoggerFactory
			.getLogger(GatewayManager.class);
	private HashMap<SocketChannel, Gateway> gateways = new HashMap<SocketChannel, Gateway>();
	private final int period;
	private LinkedBlockingQueue[] queues;
	private Random rng = new Random();
	private SeqNumManager sequencer = new SeqNumManager();
	private SimbaStoreServer server;

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	private static class Gateway {
		public Gateway(SocketChannel sc) {
			this.sc = sc;
			this.sentPings = 0;
		}

		public SocketChannel sc;
		public int sentPings;
	}

	public GatewayManager(LinkedBlockingQueue[] queues, Properties props) {
		this.period = Integer.parseInt(props.getProperty("gateway.ttl"));
		this.queues = queues;
	}

	private class PingTask extends TimerTask {

		public void run() {
			LOG.debug("Running PingTask");
			
			List<SocketChannel> deadGateways = new LinkedList<SocketChannel>();

			read.lock();
			try {
				for (Entry<SocketChannel, Gateway> entry : gateways.entrySet()) {

					Gateway gw = entry.getValue();

					if (gw.sentPings == 3) {
						gatewayFailed(gw.sc);
						deadGateways.add(gw.sc);
					} else {
						gw.sentPings++;
						sendPing(gw.sc);
					}
				}
			} finally {
				read.unlock();
			}

			write.lock();
			try {
				for (SocketChannel sc : deadGateways) {
					gateways.remove(sc);
				}
			} finally {
				write.unlock();
			}
		}
	}

	public void start() {
		LOG.debug("GatewayManger started!");
		Timer T = new Timer();
		PingTask pt = new PingTask();
		long now = System.currentTimeMillis();
		now = ((now + period - 1) / period) * period;

		T.scheduleAtFixedRate(pt, new Date(now), period);
	}

	public void addGateway(SocketChannel socket) {
		LOG.debug("Adding new gateway socket="
				+ socket.socket().getInetAddress().getHostName());
		write.lock();
		try {
			gateways.put(socket, new Gateway(socket));
		} finally {
			write.unlock();
		}
	}

	public void gatewayFailed(SocketChannel gw) {
		AbortTransaction abrt = AbortTransaction.newBuilder().build();
		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.ABORT_TRANSACTION)
				.setAbortTransaction(abrt).setSeq(sequencer.getSeq()).build();

		try {
			queues[rng.nextInt(queues.length)].put(new ServerDataEvent(gw, msg));
		} catch (InterruptedException e) {
			LOG.error("could not insert AbortTransaction message into SimbaStoreWorker queue");
			e.printStackTrace();
		}
	}

	public void sendPing(SocketChannel gw) {
		LOG.debug("PING " + gw.socket().getInetAddress().getHostName());
		Ping p = Ping.newBuilder().build();
		SimbaMessage msg = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.PING).setPing(p)
				.setSeq(sequencer.getSeq()).build();
		if(gw == null)
			LOG.error("gw is null");
		if(msg == null)
			LOG.error("msg is null");
		if(server == null)
			LOG.error("server is null");
		server.send(gw, msg.toByteArray());
	}

	public void handlePing(SocketChannel socket) {
		LOG.debug("Received PING reply from "
				+ socket.socket().getInetAddress().getHostName());
		read.lock();
		try {
			Gateway gw = gateways.get(socket);
			if (gw != null) {

				read.unlock();
				write.lock();
				try {
					gw.sentPings = 0;
				} finally {
					read.lock();
					write.unlock();
				}
			} else {
				LOG.debug("Spuring ping response");
			}

		} finally {
			read.unlock();
		}
	}

	public void cleanup(SocketChannel socket) {
		LOG.debug("Cleaning up after Gateway "
				+ socket.socket().getInetAddress().getHostName());
		write.lock();
		try {
			// remove from connected gateways list
			gateways.remove(socket);
		} finally {
			write.unlock();
		}
	}
	
	public void setServer(SimbaStoreServer server){
		this.server = server;
	}
}
