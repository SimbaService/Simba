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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.security.KeyStore;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import android.content.Context;
import android.os.Handler;
import android.util.Log;

import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.SimbaMultiMessage;

/***
 * This class manages both send and receive of Simba sync traffic.
 * 
 * @file NetworkIOHandler.java
 * @author Younghwan Go
 * @created 5:19:51 PM, Jul 30, 2012
 * @modified 2:19:13 PM, Feb 10, 2015
 */
public class NetworkIOHandler implements Runnable {
	private final String TAG = "NetworkIOHandler";

	private SimbaMessageHandler worker;
	private Selector selector;
	private List<ByteBuffer> pendingData;

	private volatile boolean shutdown;
	private AtomicBoolean sendFlag = new AtomicBoolean();
	private SocketChannel clientChannel;
	private String host; // storwire
	private int port;
	private int connectionAttempt;
	private Handler stateListener;
	private volatile boolean redoConnection = false;

	private Context context;
	private ConnectionState cs;
	private SSLEngine engine;
	private SSLContext sslContext;

	private ByteBuffer peerNetData;
	private ByteBuffer peerAppData;
	private ByteBuffer netData;
	private ByteBuffer dummy;
	private SSLEngineResult.HandshakeStatus hsStatus;
	private SSLEngineResult.Status status;
	private boolean initialHandshake;

	// Entropy related
	private static String bufstr, input;
	static char inp;
	static byte buf[];
	static ByteBuffer bbuf;
	static double ret = 0.0;
	static int[] counts = new int[256];
	static int[][] condprobs = new int[256][256];

	static int len, clen;
	static long t1, et1, et2, et3, tt1, tt2, tt3;
	static double tput1, tput2, tput3, cs1, cs2, cs3;
	static boolean verbose = false;
	static double mbps_factor = Math.pow(10, 9) / Math.pow(1024, 2);

	public NetworkIOHandler(Context c, SimbaMessageHandler smsgHandler,
			Handler stateListener, String host, int port, ConnectionState cs)
			throws IOException {
		this.context = c;
		this.worker = smsgHandler;
		this.host = host;
		this.port = port;
		this.stateListener = stateListener;

		this.pendingData = new LinkedList<ByteBuffer>();
		this.cs = cs;
		initSSLContext();
		// this.dataSizeBuf = ByteBuffer.allocate(4);
	}

	private void initSSLContext() throws IOException {
		try {
			// setup truststore to provide trust for the server certificate
			// load truststore certificate
			// Get an instance of the Bouncy Castle KeyStore format
			InputStream inTrust = context.getResources().openRawResource(
					R.raw.client_truststore);
			KeyStore trusted = KeyStore.getInstance("BKS");
			trusted.load(inTrust, "mobeeus".toCharArray());

			// initialize trust manager factory with the read truststore
			TrustManagerFactory trustManagerFactory = TrustManagerFactory
					.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trusted);

			// setup client certificate; load client certificate
			InputStream inKey = context.getResources().openRawResource(
					R.raw.client);
			KeyStore clientkeystore = KeyStore.getInstance("BKS");
			clientkeystore.load(inKey, "mobeeus".toCharArray());
			// System.out.println("Loaded client certificates: " +
			// clientkeystore.size());

			// initialize key manager factory with the read client certificate
			KeyManagerFactory keyManagerFactory = KeyManagerFactory
					.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(clientkeystore, "mobeeus".toCharArray());
			this.sslContext = SSLContext.getInstance("TLS");
			this.sslContext.init(keyManagerFactory.getKeyManagers(),
					trustManagerFactory.getTrustManagers(), null);
			// this.sslContext.init(null,
			// trustManagerFactory.getTrustManagers(), null);

		} catch (Exception e) {
			throw new IOException("Could not initialize SSLContext: "
					+ e.getMessage());
		}

	}

	private void initSSLEngine() throws IOException {
		this.engine = this.sslContext.createSSLEngine();
		this.engine.setUseClientMode(true);
		this.peerNetData = ByteBuffer.allocate(engine.getSession()
				.getPacketBufferSize());
		// TODO: Android's SSLEngine seems to be FUBAR
		// when it gives a BUFFER_OVERFLOW, it doesn't actually produce bytes
		// that can be consumed
		// The only way to deal with it is to avoid BUFFER_OVERFLOW by having a
		// large enough
		// buffer. Not really sure if this is enough.
		this.peerAppData = ByteBuffer.allocate(2 * engine.getSession()
				.getApplicationBufferSize());
		this.netData = ByteBuffer.allocate(engine.getSession()
				.getPacketBufferSize());
		// Log.v(TAG, "peerNetData=" + peerNetData.remaining());
		// Log.v(TAG, "peerAppData=" + peerAppData.remaining());
		// Log.v(TAG, "netData=" + netData.remaining());

		this.peerAppData.position(peerAppData.limit());
		this.netData.position(netData.limit());
		this.engine.beginHandshake();
		hsStatus = this.engine.getHandshakeStatus();
		initialHandshake = true;
		dummy = ByteBuffer.allocate(0);
		doHandshake();
	}

	public void updateNetworkSettings(String host, int port) {
		this.host = host;
		this.port = port;
		redoConnection = true;
	}

	// These things happen at "construction" time, except that we want them to
	// be executed
	// in the IO thread
	private void onStart() throws IOException {
		initSSLContext();
	}

	// just make it modular for reuse
	private void init_connection() {
		try {
			assert this.clientChannel == null : "Must only be called when there is no connection";

			if (connectionAttempt != 0) {
				long sleepTime = Math.min(5000 * connectionAttempt,
						Preferences.MAX_CONNECT_TIMER);
				try {
					Log.v(TAG, "Sleeping for " + sleepTime
							+ " ms before attempting connection");
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {

				}
			}
			connectionAttempt++;

			this.clientChannel = SocketChannel.open();
			this.clientChannel.configureBlocking(false);

			// Use SSL socket instead
			Socket socket = this.clientChannel.socket();
			socket.setTcpNoDelay(true);
			socket.setKeepAlive(true);

			Log.v(TAG, "Connecting to host " + host + ":" + port);
			InetSocketAddress isa = new InetSocketAddress(host, port);
			this.clientChannel.connect(isa);
			this.selector = SelectorProvider.provider().openSelector();
			this.clientChannel.register(this.selector, SelectionKey.OP_CONNECT);
		} catch (Exception e) {
			Log.e(TAG, "Could not connect");
		}
	}

	// for up-link traffic
	private void sendInternal(CompressedResult cr, boolean first) {
		// Log.v(TAG, "sending" + (first ? " first" : "") + " len=" +
		// data.length + " bytes");

		ByteBuffer tmp;
		int compress_header;

		// We have to do a datacopy because the SSLEngine interface does not
		// support an efficient scatter gather interface. It wraps each
		// ByteBuffer
		// put into it as a separate message, which makes it less space
		// efficient.
		tmp = ByteBuffer.allocate(cr.returnData.length + 4);

		assert cr.returnData.length < Integer.MAX_VALUE : "MSB full for compression state: "
				+ cr.returnData.length + " = " + Integer.MAX_VALUE;
		if (cr.compressed) {
			compress_header = cr.returnData.length | (1 << 30); // set MSB to
																// indicate
																// compressed;
																// remember to
																// get MSB and
																// unset in
																// server before
																// use
			Log.d(TAG, "Compressed data of length " + cr.returnData.length
					+ ": header = " + (((compress_header & 0xFFFF) >> 30) > 0));
		} else {
			compress_header = cr.returnData.length & ~(1 << 30); // unset MSB to
																	// indicate
																	// uncompressed
			Log.d(TAG, "Uncompressed data of length " + cr.returnData.length
					+ ": header = " + (((compress_header & 0xFFFF) >> 30) > 0));
		}

		tmp.putInt(compress_header);
		tmp.put(cr.returnData);
		tmp.flip();

		synchronized (this.pendingData) {

			if (first)
				this.pendingData.add(0, tmp);
			else
				this.pendingData.add(tmp);
		}
		SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.UP, tmp.remaining(), 
				"Compressed: "+ cr.compressed +"," + cr.returnData.length + ",uq");
		this.sendFlag.set(true);
		Log.v(TAG, "buffered " + (first ? "first " : " ") + tmp.remaining()
				+ " bytes...");
		// SimbaLogger.log("uq," + tmp.remaining());

		if (this.selector != null)
			this.selector.wakeup();
	}

	public static double CalcEntropy_1(String buf) { // First-order Model
		Arrays.fill(counts, 0);
		int i = 0, j = 0, total = 0;
		double p = 0.0, lp = 0.0, sum = 0.0;

		for (i = 0; i < len; i++) {
			j = (int) buf.charAt(i);
			if (j >= 0 && j < 256) {
				counts[j]++;
				total++;
			}
		}

		for (j = 0; j < 256; j++) {
			if (counts[j] == 0)
				continue;
			p = 1.0 * counts[j] / total;
			lp = Math.log(p) / Math.log(2); // lp bits needed to distinguish p
											// states
			sum -= p * lp; // for p such entries, we need p*lp bits
		}
		return sum;
	}

	public static double CalcEntropy_2(String buf) { // Second-order Model

		len = buf.length();
		int i = 0, j = 0, total = 0;
		double p = 0.0, lp = 0.0, sum = 0.0, bigsum = 0.0;
		int lastchar = -1;
		for (j = 0; j < 256; j++) {
			Arrays.fill(condprobs[j], 0);
		}
		Arrays.fill(counts, 0);

		// condprobs[i][j] gives the prob. char[j] given char[i]
		for (i = 0; i < len; i++) {
			j = (int) buf.charAt(i);
			if (j >= 0 && j < 256) {
				counts[j]++;
				if (lastchar >= 0 && lastchar < 256) {
					condprobs[j][lastchar]++;
					total++;
				}
			}
			lastchar = j;
		}

		for (i = 0; i < 256; i++) {
			sum = 0.0;
			for (j = 0; j < 256; j++) {
				if (condprobs[j][i] == 0)
					continue;
				p = 1.0 * condprobs[j][i] / total;
				lp = Math.log(p) / Math.log(2); // lp bits needed to distinguish
												// p states
				sum -= p * lp; // for p such entries, we need p*lp bits
			}
			bigsum += sum * 1.0 * counts[i] / len;
		}
		return bigsum;
	}

	public static double roundTwoDecimals(double d) {

		DecimalFormat twoDForm = new DecimalFormat("#.###");
		return Double.valueOf(twoDForm.format(d));
	}

	public boolean entropyCompressable(byte[] input_data) {

		// calculate entropy 1st order
		double e_1 = CalcEntropy_1(input_data.toString());
		double e_2 = CalcEntropy_2(input_data.toString());
		Log.d(TAG, "E_1 = " + e_1 + " : E_2 = " + e_2);
		if (e_1 > 0 && e_2 > 0) {
			// for now simply return true -> compress
			return true;
		} else
			return false;
	}

	public CompressedResult compressor(byte[] input_data) {

		if (Preferences.WRITE_COMPRESS) {
			if (Preferences.COMPRESS_HEURISTIC == Preferences.COMPRESS_CHOICE.COMPRESS_HEURISTIC_SIZE.getStatusCode()) {
				// decide whether or not to compress based on size
				if (input_data.length < Preferences.COMPRESSABLE_SIZE) {
					return (new CompressedResult(input_data, false));
				} else {
					Log.d(TAG, "Compressing " + input_data.length
							+ " bytes of data based on size");
				}
			} else if (Preferences.COMPRESS_HEURISTIC == Preferences.COMPRESS_CHOICE.COMPRESS_HEURISTIC_ENTROPY.getStatusCode()) {
				// decide whether or not to compress based on entropy
				if (!entropyCompressable(input_data))
					return (new CompressedResult(input_data, false));
				else {
					Log.d(TAG, "Compressing " + input_data.length
							+ " bytes of data based on entropy");
				}
			}

			ByteArrayOutputStream compress_bos = new ByteArrayOutputStream();
			DeflaterOutputStream dos = new DeflaterOutputStream(compress_bos);
			try {
				// bos.writeTo(dos);
				dos.write(input_data, 0, input_data.length);
				dos.finish();

			} catch (IOException e) {
				Log.d(TAG, "Failed to compress");
				e.printStackTrace();
			}
			// Log.d("TAG", "Compressed from " + input_data.length + " to " +
			// compress_bos.size());
			return (new CompressedResult(compress_bos.toByteArray(), true));

		} else {
			// Log.d("TAG", "not compressing data size: " + input_data.length);
			return (new CompressedResult(input_data, false));
		}
	}

	public void send(ByteArrayOutputStream bos) {
		byte[] data = bos.toByteArray();
		sendInternal(compressor(data), false);
	}

	private boolean finish_connect(SelectionKey key) throws IOException {
		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			if (clientChannel.isConnectionPending()) {
				while (!clientChannel.finishConnect()) {
					Log.v(TAG, "Connection still pending, sleep");
					Thread.sleep(1000);
				}
			}
		} catch (IOException e) {
			Log.v(TAG, "IOException while finishing connection, giving up");
			e.printStackTrace();
			// Cancel the channel's registration with our selector
			forceClose();
			return false;
		} catch (InterruptedException e) {
			forceClose();
			return false;
		}
		Log.v(TAG,
				"finish_connect(): channel is connected? "
						+ clientChannel.isConnected());
		this.initSSLEngine();

		// Register an interest in read on this channel
		key.interestOps(SelectionKey.OP_READ);
		connectionAttempt = 0;
		// initSSLEngine();
		return true;

	}

	private ByteBuffer payloadBuf = ByteBuffer.allocate(4);
	private ReadBuffer readMessageBuf;
	private int rmbOffset;

	private void handleRead() throws IOException {

		if (initialHandshake)
			doHandshake();
		else if (shutdown)
			doShutdown();
		else
			processUserReads();
	}
    static class ReadBuffer {
        byte[] buf;
        boolean isCompressed;
        public ReadBuffer(int len, boolean isCompressed) {
            buf = new byte[len];
            this.isCompressed = isCompressed;
        }
    }

	private void processUserReads() throws IOException {

		do {

			if (!peerAppData.hasRemaining()) {
				int appBytesProduced = readAndUnwrap();
				if (appBytesProduced == 0) {
					// Log.v(TAG, "pur register r");
					SelectionKey key = this.clientChannel.keyFor(this.selector);
					key.interestOps(SelectionKey.OP_READ);

					return;
				}
			}
			// Log.v(TAG, "read peerAppData remaining=" +
			// peerAppData.remaining());
			if (readMessageBuf == null) {
				assert payloadBuf.hasRemaining() : "We must still be waiting for the length field";
				int toCopy = Math.min(peerAppData.remaining(),
						payloadBuf.remaining());
				payloadBuf.put(peerAppData.array(), peerAppData.arrayOffset()
						+ peerAppData.position(), toCopy);
				peerAppData.position(peerAppData.position() + toCopy);
				if (!payloadBuf.hasRemaining()) {
					int l = payloadBuf.getInt(0);
                    boolean isCompressed = (l >> 30) > 0;
                    l &= ~(1 << 30);
                    Log.v(TAG, "Allocating for reading=" +
       					 payloadBuf.getInt(0));
					readMessageBuf = new ReadBuffer(l, isCompressed);
					payloadBuf.clear();
				}
			}

			if (readMessageBuf != null) {
				int toCopy = Math.min(readMessageBuf.buf.length - rmbOffset,
						peerAppData.remaining());
				peerAppData.get(readMessageBuf.buf, rmbOffset, toCopy);
				rmbOffset += toCopy;
				if (rmbOffset == readMessageBuf.buf.length) {

					if (readMessageBuf.isCompressed) {
						byte[] tmp_data = new byte[Preferences.READ_COMPRESS_BUFSIZE];
						byte[] decompress_data = null;
						int iis_avail = 0, bis_avail = 0;

						ByteArrayInputStream bis = new ByteArrayInputStream(
								readMessageBuf.buf);
						InflaterInputStream iis = new InflaterInputStream(bis);
						ByteArrayOutputStream decompress_os = new ByteArrayOutputStream();

						try {
							iis_avail = iis.available();
							bis_avail = bis.available();
							if (iis_avail > 0 && bis_avail > 0) {
								do {

									// Log.d(TAG,
									// "About to read from decompressed stream iis "
									// + iis_avail + ":" + bis_avail);
									int len = iis.read(tmp_data, 0,
											tmp_data.length);
									// Log.d(TAG, "Length = " + len);
									if (len > 0)
										decompress_os.write(tmp_data, 0, len);
									// Log.d(TAG,
									// "Decompressing ................................");
								} while (iis.available() != 0);

								decompress_data = decompress_os.toByteArray();
								SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.DN, decompress_data.length,
										"Decompressed, Compressed: " + readMessageBuf.buf.length);
								// Log.d(TAG, "Decompressed from " +
								// readMessageBuf.buf.length + " to " +
								// decompress_data.length);
								readMessageBuf = null;
								rmbOffset = 0;
								this.worker.process(decompress_data);
							}
							/*
							 * else { Log.d(TAG,
							 * "NOthing to read from input stream to be decompressed"
							 * ); }
							 */
						} catch (IOException e) {
							e.printStackTrace();
						}

					} else {
						byte[] tmp = readMessageBuf.buf;
						SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.DN, readMessageBuf.buf.length,
								"Decompressed, Compressed: " + readMessageBuf.buf.length);
						
						readMessageBuf = null;
						rmbOffset = 0;
						this.worker.process(tmp);
					}
				}
			}
		} while (true);

	}

	/*
	 * private boolean isOnline() { ConnectivityManager cm =
	 * (ConnectivityManager)
	 * context.getSystemService(Context.CONNECTIVITY_SERVICE); NetworkInfo
	 * net_info = cm.getActiveNetworkInfo(); if (net_info != null &&
	 * net_info.isConnected()) { return true; } return false; }
	 */

	public void run() {

		try {
			this.onStart();
		} catch (IOException e) {
			Log.e(TAG, "Could not initialize NetworkIOHandler, giving up");
			return;
		}
		while (true) {
			try {
				// Log.v(TAG, "run() loop");

				ConnState currentConnState = cs.getConnectionState();
				// Log.d(TAG, "Current n/w state " +
				// currentConnState.toString());

				// =====================================
				// Handle reconnection issue
				// if (!isOnline()) {
				if (currentConnState == ConnState.NONE) {
					Thread.sleep(Preferences.RECONNECT_TIMER); // check network
																// every N sec
					continue;
				}

				if (redoConnection) {
					Log.d(TAG, "Application requested a new connection");
					redoConnection = false;
					forceClose();
					continue;
				}

				if (clientChannel == null) {
					init_connection();
					continue;
				}

				if (!clientChannel.isConnected()) {
					if (!clientChannel.isConnectionPending()) {
						forceClose();
						init_connection();
						Log.v(TAG, "client reconnect");
					} else {
						Log.v(TAG, "client connection pending");
						SelectionKey key = clientChannel.keyFor(this.selector);
						if (!finish_connect(key))
							continue;

					}
				} else {
					// Log.v(TAG, "client connected");
					// =====================================

					if (sendFlag.getAndSet(false)) {
						SelectionKey key = this.clientChannel
								.keyFor(this.selector);
						if (key == null) {
							// Log.v(TAG, "1. register rw");
							key = this.clientChannel.register(this.selector,
									SelectionKey.OP_WRITE
											| SelectionKey.OP_READ);
						} else {
							// Log.v(TAG, "2. register rw");

							key = key.interestOps(SelectionKey.OP_WRITE
									| SelectionKey.OP_READ);
						}

					}
				}
				
				if (this.selector == null) {
					continue;
				}

				// Wait for an event for one of the registered channels
				// Log.v(TAG, "Going into select registered=" +
				// this.clientChannel.isRegistered());
				this.selector.select();

				// Iterate over the set of keys for which events are available
				Set<SelectionKey> keys = this.selector.selectedKeys();
				Iterator<SelectionKey> selectedKeys = keys.iterator();
				// Log.v(TAG, "|keys|=" + keys.size());
				while (selectedKeys.hasNext()) {
					SelectionKey key = (SelectionKey) selectedKeys.next();
					selectedKeys.remove();

					if (!key.isValid()) {
						continue;
					}
					// Log.v(TAG, "readyOps=" + key.readyOps());

					// Check what event is available and deal with it
					if (key.isConnectable()) {
						// Log.v(TAG, "run() selector event connect");
						this.finish_connect(key);
					}

					if (key.isReadable()) {
						// Log.v(TAG, "run() selector event read");
						this.handleRead();
					}

					if (key.isWritable()) {
						// Log.v(TAG, "run() selector event write");
						this.handleWrite();
						// try a read for good measure
						this.handleRead();
					}
				}
				// Log.v(TAG, "remaing |keys|=" +
				// this.selector.selectedKeys().size());
			} catch (IOException e) {
				Log.e(TAG,
						"Exception occurred during communication, closing channel: "
								+ e);
				forceClose();
				// System.exit(1);
				stateListener
						.sendEmptyMessage(InternalMessages.NETWORK_DISCONNECTED);
				// e.printStackTrace();
			} catch (InterruptedException e) {
				Log.v(TAG, "Network thread interrupted");
				forceClose();
				stateListener
						.sendEmptyMessage(InternalMessages.NETWORK_DISCONNECTED);

				break;
			}

		}
	}

	private void handleWrite() throws IOException {
		boolean canSendMore = true;
		if (netData.hasRemaining())
			canSendMore = flushData();
		if (canSendMore) {
			// Log.v(TAG, "initialHandshake=" + initialHandshake);
			// the buffer was sent completely
			if (initialHandshake)
				doHandshake();
			else if (shutdown)
				doShutdown();
			else {
				// Log.v(TAG, "processingUserWrites");

				// now we are good to process user writes
				processUserWrites();
			}
		} else {
			// data is not completely flushed, wait for another day
		}
	}

	private boolean wrapSomeMore(ByteBuffer buf) throws IOException {
		try {
			SSLEngineResult res = engine.wrap(buf, netData);
			// Log.v(TAG, "wrap res=" + res);
			return res.getStatus() == SSLEngineResult.Status.OK;
		} catch (SSLException e) {
			throw new IOException(e.getMessage());
		}
	}

	void processUserWrites() throws IOException {

		assert !netData.hasRemaining() : "All net data must be sent by now";
		netData.clear();

		while (true) {
			// Log.v(TAG, "processUserWrites loop");
			ByteBuffer toSend = null;
			synchronized (this.pendingData) {

				while (!this.pendingData.isEmpty()) {
					toSend = this.pendingData.get(0);
					if (!toSend.hasRemaining()) {
						this.pendingData.remove(0);
						toSend = null;
					} else
						break;
				}

				// send flag might have been set again
				// during this looping, so we reset it if
				// if we've chewed through all data to send
				if (this.pendingData.isEmpty())
					this.sendFlag.set(false);

			}
			if (toSend == null)
				break;

			// Log.v(TAG, "Ready to wrap and send " + bytesToHex(toSend.array())
			// + " remaining=" + toSend.remaining());
			while (toSend.hasRemaining()) {

				try {
					// assert !netData.hasRemaining() :
					// "All net data must be sent by now";

					// we don't really care about whether we could wrap all of
					// toSend or not. We will send out whatever we can wrap
					// if we could have accommodated more messages, they'll be
					// flushed separately
					// Log.v(TAG, "Wrapping " + toSend.remaining() +
					// " bytes to send");
					netData.clear();

					boolean ret = wrapSomeMore(toSend);
					// /Log.v(TAG, "wrapping ret=" + ret);
					netData.flip();
					if (!flushData()) {

						// Log.v(TAG,
						// "Socket cannot handle any more data right now");
						return;
					}
				} catch (IOException e) {
					// could not send out toSend, restore its state and throw
					// exception
					// also, don't remove it from the pending list
					toSend.position(0);
					throw e;
				}
			}

		}

	}

	public void send(SimbaMultiMessage mm) {
		try {
			byte[] buf = mm.toByteArray();
			SimbaLogger.log(SimbaLogger.Type.MMMSG, SimbaLogger.Dir.UP, mm.computeSize(), "bufsize (Decompressed): " + buf.length + ", msgs: "  
					+ mm.getMessages().size() + ", " + mm.hashCode());
			
			/* Added for logging NA */
			List<SimbaMessage> msgs = mm.getMessages();
			for (SimbaMessage m : msgs) {
				SimbaLogger.log(SimbaLogger.Type.MMSG, SimbaLogger.Dir.UP, m.computeSize(), 
						SimbaMessage.Type.getStringValue(m.getType()) + " : " + m.getType() + ", " + mm.hashCode());
			}
			
			// SimbaLogger.log("u," + buf.length);
			Log.v(TAG, "Sending mm size: " + buf.length);
			this.sendInternal(compressor(buf), false);
		} catch (IOException e) {
			Log.v(TAG, "Exception while serializing messsage " + mm);
		}

	}

	public void send(SimbaMessage m) {
		//SimbaLogger.log(SimbaLogger.Type.MMSG,SimbaLogger.Dir.UP, m.computeSize(),
				//SimbaMessage.Type.getStringValue(m.getType()) + " : " + m.getType());
		this.send(SimbaMultiMessage.newBuilder().addElementMessages(m).build());
	}

	public void sendFirst(SimbaMessage m) {
		try {
			Log.v(TAG, "Sending: " + m);
			SimbaLogger.log(SimbaLogger.Type.FIRST, SimbaLogger.Dir.UP, m.computeSize(),
					SimbaMessage.Type.getStringValue(m.getType()) + " : " + m.getType());

			// SimbaLogger.log("u," + m.getSeq() + "," + m.getSeq() + "," +
			// m.computeSize());
			this.sendInternal(compressor(SimbaMultiMessage.newBuilder()
					.addElementMessages(m).build().toByteArray()), true);
		} catch (IOException e) {
			Log.v(TAG, "Exception while serializing messsage " + m);
		}
	}

	private void doHandshake() throws IOException {
		// System.out.println(Thread.currentThread().getName() +
		// " doHandshake called");
		// Thread.dumpStack();

		boolean looping = true;
		while (looping) {
			SSLEngineResult res;
			// System.out.println("hsstatus=" + hsStatus);
			switch (hsStatus) {
			case NOT_HANDSHAKING:
			case FINISHED: {
				if (initialHandshake)
					finishInitialHandshake();
				looping = false;
				break;
			}
			case NEED_TASK: {
				doTasks();
				break;

			}
			case NEED_UNWRAP: {
				readAndUnwrap();
				// System.out.println(Thread.currentThread().getName() +
				// "doHandshake unwrap returned " + ret);
				// Thread.dumpStack();
				if (initialHandshake
						&& status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {

					// System.out.println(Thread.currentThread().getName() +
					// " Still not done handshaking, and waiting for more data to appear");
					SelectionKey key = this.clientChannel.keyFor(this.selector);

					key.interestOps(SelectionKey.OP_READ);
					looping = false;
				}

				break;

			}
			case NEED_WRAP: {
				if (netData.hasRemaining()) {

					Log.v(TAG, "NEED_WRAP but netData hasRemaining");
					return;
				}

				netData.clear();
				res = engine.wrap(dummy, netData);
				hsStatus = res.getHandshakeStatus();
				netData.flip();
				if (!flushData())
					looping = false;
				break;
			}

			}

		}

	}

	private int readAndUnwrap() throws IOException {
		assert !peerAppData.hasRemaining() : "Application buffer not empty";
		// Log.v(TAG, "before readFromSocket peerNetData remaining: " +
		// peerNetData.remaining());
		int bytesRead;

		int numTries = 0;
		do {
			++numTries;
			bytesRead = this.clientChannel.read(peerNetData);
			// Log.v(TAG, "readFromSocket: " + bytesRead);
			if (bytesRead == 0) {
				if (numTries == 5)
					return 0;
			} else {
				SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.DN, bytesRead, "Compressed, d");
			}

		} while (bytesRead == 0 && peerNetData.hasRemaining());
		/*
		 * int tot = 0; do { bytesRead = this.clientChannel.read(peerNetData);
		 * Log.v(TAG, "bytesRead=" + bytesRead);
		 * 
		 * if (bytesRead == 0 && tot == 0) return 0; tot += bytesRead; } while
		 * (bytesRead > 0 && peerNetData.hasRemaining());
		 */
		// Log.v(TAG, "readFromSocket: " + bytesRead);
		if (bytesRead == -1) {
			// The remote side closed the connection, do the cleanup on our side
			// as well
			engine.closeInbound();
			if (peerNetData.position() == 0
					|| status == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
				return -1;
			}
			// Although we reached EOF, we still have some data left to be
			// decrypted, we must process it
		}

		peerAppData.clear();
		peerNetData.flip();
		SSLEngineResult res;
		do {
			res = engine.unwrap(peerNetData, peerAppData);
			// Log.v(TAG, "res 1 unwrap=" + res);

			// During an handshake renegotiation we might need to perform
			// several unwraps to consume the handshake data.
		} while (res.getStatus() == SSLEngineResult.Status.OK
				&& res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
				&& res.bytesProduced() == 0);

		// If the initial handshake finish after an unwrap, we must activate
		// the application interestes, if any were set during the handshake
		if (res.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
			finishInitialHandshake();
		}

		// While the status is OK and there is still more unprocessed data,
		// process it
		while (// peerAppData.position() == 0 &&
		res.getStatus() == SSLEngineResult.Status.OK
				&& peerNetData.hasRemaining()) {
			res = engine.unwrap(peerNetData, peerAppData);
			// Log.v(TAG, "res 2 unwrap=" + res);

			// Log.v(TAG, "unwrap 2 successful");

		}

		/*
		 * The status may be: OK - Normal operation OVERFLOW - Should never
		 * happen since the application buffer is sized to hold the maximum
		 * packet size. UNDERFLOW - Need to read more data from the socket. It's
		 * normal. CLOSED - The other peer closed the socket. Also normal.
		 */
		status = res.getStatus();
		hsStatus = res.getHandshakeStatus();
		// Should never happen, the peerAppData must always have enough space
		// for an unwrap operation
		assert status != SSLEngineResult.Status.BUFFER_OVERFLOW : "Buffer should not overflow: "
				+ res.toString();

		// The handshake status here can be different than NOT_HANDSHAKING
		// if the other peer closed the connection. So only check for it
		// after testing for closure.
		if (status == SSLEngineResult.Status.CLOSED) {
			shutdown = true;
			doShutdown();
			return -1;
		}

		// Prepare the buffer to be written again.
		peerNetData.compact();
		// And the app buffer to be read.
		peerAppData.flip();
		/*
		 * if (hsStatus == SSLEngineResult.HandshakeStatus.NEED_TASK || hsStatus
		 * == SSLEngineResult.HandshakeStatus.NEED_WRAP || hsStatus ==
		 * SSLEngineResult.HandshakeStatus.FINISHED) { Log.v(TAG,
		 * "Rehandshaking..."); doHandshake(); }
		 */
		return peerAppData.remaining();

	}

	private void doShutdown() throws IOException {

		assert !netData.hasRemaining() : "Buffer was not empty";
		if (engine.isOutboundDone()) {
			try {
				this.clientChannel.close();
			} catch (IOException e) {
				removeChannel();
			}
			return;
		}

		netData.clear();
		try {
			engine.wrap(dummy, netData);
		} catch (SSLException e1) {
			Log.w(TAG, "Error during shutdown: " + e1);
			try {
				this.clientChannel.close();
			} catch (IOException e) {
				removeChannel();
			}
			return;
		}
		netData.flip();
		flushData();
		removeChannel();
	}

	private void removeChannel() {
		try {
			SelectionKey key = this.clientChannel.keyFor(this.selector);
			if (key != null)
				key.cancel();
			this.engine = null;
			this.clientChannel.close();
			this.clientChannel = null;
			if (this.selector != null) {
				this.selector.close();
				this.selector = null;
			}
			if (netData != null) {
				netData.clear();
				netData = null;
			}

		} catch (IOException e) {
			Log.w(TAG, "Exception during channel shutdown: " + e);
		}
	}

	private void forceClose() {
		try {
			if (engine != null) {
				netData.position(netData.limit());
				doShutdown();
			} else if (clientChannel != null) {
				removeChannel();
			}
		} catch (IOException e) {
			Log.w(TAG, "Exception during force close: " + e);
		} finally {
			this.engine = null;
			this.clientChannel = null;
			this.selector = null;
		}
	}

	private void finishInitialHandshake() throws IOException {
		initialHandshake = false;
		SelectionKey key = this.clientChannel.keyFor(this.selector);

		if (sendFlag.get()) {
			// Log.v(TAG, "3. register rw");

			key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		} else {
			// Log.v(TAG, "3. register r");

			key.interestOps(SelectionKey.OP_READ);
		}
		stateListener.sendEmptyMessage(InternalMessages.NETWORK_CONNECTED);
	}

	private static String bytesToHex(byte[] bytes) {
		final char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8',
				'9', 'A', 'B', 'C', 'D', 'E', 'F' };
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	private boolean flushData() throws IOException {
		assert netData.hasRemaining() : "Trying to write but netData buffer is empty";
		int written;
		try {
			// Log.v(TAG, "Attempting to write" + netData.remaining());
			written = this.clientChannel.write(netData);
			SimbaLogger.log(SimbaLogger.Type.BYTES, SimbaLogger.Dir.UP, written, "uf");
			// Log.v(TAG, "writtenToSocket: " + written);
		} catch (IOException e) {
			netData.position(netData.limit());
			throw e;
		}
		SelectionKey key = this.clientChannel.keyFor(this.selector);

		if (netData.hasRemaining()) {
			// Log.v(TAG, "register rw");

			key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
			return false;
		} else {
			// Log.v(TAG, "register r");

			key.interestOps(SelectionKey.OP_READ);
			return true;
		}
	}

	private void doTasks() {
		Runnable task;
		while ((task = engine.getDelegatedTask()) != null) {
			task.run();
		}
		hsStatus = engine.getHandshakeStatus();
	}

}
