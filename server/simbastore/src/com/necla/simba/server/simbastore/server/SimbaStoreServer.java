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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.necla.simba.nio.ChangeRequest;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.simbastore.server.data.ServerDataEvent;
import com.necla.simba.util.ConsistentHash;

public class SimbaStoreServer implements Runnable {
	private static final Logger LOG = LoggerFactory
			.getLogger(SimbaStoreServer.class);

	// The host:port combination to listen on
	private InetAddress hostAddress;
	private int port;

	// private SimbaStoreWorker worker;

	// The channel on which we'll accept connections
	private ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	private Selector selector;

	class ConnState {
		ByteBuffer incomingSize = ByteBuffer.allocate(4);
		ByteBuffer data = null;
	}
	

	// A list of PendingChange instances
	private List<ChangeRequest> pendingChanges = new LinkedList<ChangeRequest>();

	// Maps a SocketChannel to a list of ByteBuffer instances
	private Map<SocketChannel, List<ByteBuffer>> pendingData = new HashMap<SocketChannel, List<ByteBuffer>>();

	private LinkedBlockingQueue[] queues;
	private ConsistentHash hasher;

	private GatewayManager gm;

	public SimbaStoreServer(InetAddress hostAddress, int port,
			LinkedBlockingQueue[] queues, ConsistentHash hasher,
			GatewayManager gm/* SimbaStoreWorker worker */) throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		this.selector = this.initSelector();
		this.queues = queues;
		this.hasher = hasher;
		this.gm = gm;
		// this.worker = worker;
	}

	public void send(SocketChannel socket, byte[] data) {

		ByteBuffer toSend = ByteBuffer.allocate(4 + data.length);
		toSend.putInt(data.length).put(data).flip();

		ChangeRequest writeRequest = new ChangeRequest(socket,
				ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE);

		// And queue the data we want written
		synchronized (this.pendingData) {
			List<ByteBuffer> queue = this.pendingData.get(socket);
			if (queue == null) {
				queue = new LinkedList<ByteBuffer>();
				this.pendingData.put(socket, queue);
			}
			queue.add(toSend);
		}

		synchronized (this.pendingChanges) {
			// Indicate we want the interest ops set changed
			this.pendingChanges.add(writeRequest);
		}

		// Finally, wake up our selecting thread so it can make the required
		// changes
		this.selector.wakeup();
	}

	public void run() {
		LOG.info("Started SimbaStoreServer on " + this.hostAddress + ":"
				+ this.port);

		while (true) {
			try {
				// Process any pending changes
				synchronized (this.pendingChanges) {
					Iterator<ChangeRequest> changes = this.pendingChanges
							.iterator();
					while (changes.hasNext()) {
						ChangeRequest change = (ChangeRequest) changes.next();
						switch (change.type) {
						case ChangeRequest.CHANGEOPS:
							SelectionKey key = change.socket
									.keyFor(this.selector);
							key.interestOps(change.ops);
						}
					}
					this.pendingChanges.clear();
				}

				// Wait for an event one of the registered channels
				int selected = this.selector.select();

				if (selected == 0) {
					// Go back to the beginning of the loop
					continue;
				}

				// Iterate over the set of keys for which events are available
				Iterator selectedKeys = this.selector.selectedKeys().iterator();
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid()) {
						continue;
					}

					// Check what event is available and deal with it
					if (key.isAcceptable()) {
						// System.out.println("accept");
						this.accept(key);
					} else if (key.isReadable()) {
						// System.out.println("read");
						this.read(key);
					} else if (key.isWritable()) {
						// System.out.println("write");
						this.write(key);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void accept(SelectionKey key) throws IOException {
		// For an accept to be pending the channel must be a server socket
		// channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key
				.channel();

		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		socketChannel.configureBlocking(false);

		Socket socket = socketChannel.socket();
		socket.setTcpNoDelay(true);
		socket.setKeepAlive(true);

		LOG.info("Accepted connection from "
				+ socket.getInetAddress().getHostName() + " on port "
				+ socket.getPort());

		this.gm.addGateway(socketChannel);
		LOG.info("Adding " + socket.getInetAddress().getHostName()
				+ " to GatewayManager");

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ, new ConnState());
	}

	@SuppressWarnings("unchecked")
	private void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		int numRead = 0;
		ConnState c = (ConnState) key.attachment();

		if (c.data == null) {

			// read size of message
			c.incomingSize.clear();

			try {
				numRead = socketChannel.read(c.incomingSize);
			} catch (IOException e) {
				// The remote forcibly closed the connection, cancel
				// the selection key and close the channel.
				key.attach(null);
				key.cancel();
				socketChannel.close();
				return;
			}

			if (numRead == -1) {
				// Remote entity shut the socket down cleanly. Do the
				// same from our end and cancel the channel.
				LOG.info("Host "
						+ socketChannel.socket().getInetAddress().getHostName()
						+ " has closed the connection. Closing socket.");

				// cleanup after this gateway
				LOG.info("Removing " + socketChannel.socket().getInetAddress().getHostName()
						+ " from GatewayManager");
				this.gm.cleanup(socketChannel);
				
				key.channel().close();
				key.cancel();
				return;
			}

			c.incomingSize.flip();
			int toRead = c.incomingSize.getInt();
			LOG.debug("Need " + toRead + " from " + socketChannel.socket());

			c.data = ByteBuffer.allocate(toRead);
		}

		try {
			numRead = socketChannel.read(c.data);
			LOG.debug("Read " + numRead + " from " + socketChannel.socket());
		} catch (IOException e) {
			// The remote forcibly closed the connection, cancel
			// the selection key and close the channel.
			key.cancel();
			socketChannel.close();
			return;
		}

		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			LOG.info("Host "
					+ socketChannel.socket().getInetAddress().getHostName()
					+ " has closed the connection. Closing socket.");
			key.channel().close();
			key.cancel();
			return;
		}

		if (c.data.remaining() > 0) {
			LOG.debug("message unifinished; " + c.data.remaining()
					+ " bytes left");
			return;
		} else {
			c.data.flip();

			SimbaMessage msg = null;
			try {
				msg = SimbaMessage.parseFrom(c.data.array());
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
			LOG.debug("Got msg type=" + msg.getType() + " seq=" + msg.getSeq() + "from " + socketChannel.socket());

			int q = hasher.hash(msg.getToken(), queues.length);

			try {
				queues[q].put(new ServerDataEvent(socketChannel, msg));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		c.data = null;
	}

	private void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.pendingData) {
			List<ByteBuffer> queue = (LinkedList<ByteBuffer>) this.pendingData
					.get(socketChannel);

			// This method has been changed to write only 1 item from the socket
			// channels queue. Otherwise, clients may not be serviced fairly

			// Write until there's not more data ...
			while (!queue.isEmpty()) {
				ByteBuffer buf = (ByteBuffer) queue.get(0);
				socketChannel.write(buf);
				if (buf.remaining() > 0) {
					// ... or the socket's buffer fills up
					LOG.debug("socket buffer full: will read remaining bytes later");
					 break;
				}
				queue.remove(0);
			}

			if (queue.isEmpty()) {
				// We wrote away all data, so we're no longer interested
				// in writing on this socket. Switch back to waiting for
				// data.
				key.interestOps(SelectionKey.OP_READ);
			}
		}
	}

	private Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		// Bind the server socket to the specified address and port
		InetSocketAddress isa = new InetSocketAddress("0.0.0.0",//this.hostAddress,
				this.port);
		serverChannel.socket().bind(isa);

		// Register the server socket channel, indicating an interest in
		// accepting new connections
		serverChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

		return socketSelector;
	}
}
