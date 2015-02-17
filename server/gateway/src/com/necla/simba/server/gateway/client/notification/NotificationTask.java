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
package com.necla.simba.server.gateway.client.notification;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;
import com.necla.simba.client.SeqNumManager;
import com.necla.simba.protocol.Client.BitmapNotify;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.Notify;
import com.necla.simba.server.gateway.client.notification.NotificationManager;
import com.necla.simba.server.gateway.Main;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.subscription.Subscription;
import com.necla.simba.util.Pair;

public class NotificationTask extends TimerTask {
	private int period;
	private static boolean BITMAP_NOTIFICATION = Boolean
			.parseBoolean(Main.properties.getProperty("bitmap.notification"));
	SeqNumManager sequencer;
	NotificationManager nom;

	public NotificationTask(int p, SeqNumManager sequencer,
			NotificationManager parent) {
		this.period = p;
		this.sequencer = sequencer;
		this.nom = parent;
	}

	public void run() {
		List<Pair<Subscription, ClientState>> list = nom.getBucket(period);

		synchronized (list) {
			for (Pair<Subscription, ClientState> pair : list) {
				Subscription sub = pair.getFirst();
				ClientState cs = pair.getSecond();
				BitSet notification = null;

				// only send notification if client is connected

				if (cs.isConnected) {

					cs.bitmapRead.lock();
					cs.subscriptionRead.lock();

					try {
						// get a pointer to this client's bitmap
						BitSet bitmap = cs.getBitmap(); // synchronized call

						// get a pointe to this client's subscriptions
						ArrayList<Subscription> subscriptions = cs.subscriptions;

						// create a new temp bitset for this notification
						notification = new BitSet(subscriptions.size());

						// for all of this client's subscriptions
						for (int i = 0; i < subscriptions.size(); i++) {

							// only set bits that are set and belong to this
							// period bucket

							if (bitmap.get(i)
									&& period == subscriptions.get(i).period) {

								// set bit
								notification.set(i);

								cs.bitmapRead.unlock();
								cs.bitmapWrite.lock();
								try {
									// clear that bit in client's map
									bitmap.clear(i);
								} finally {
									cs.bitmapRead.lock();
									cs.bitmapWrite.unlock();
								}
							}
						}
					} finally {
						cs.subscriptionRead.unlock();
						cs.bitmapRead.unlock();
					}
				}

				// send bitmap, if any updates
				if (notification.cardinality() > 0) {
					// queue bitmap to server send queue

					ClientMessage.Builder m = ClientMessage.newBuilder();
					if (BITMAP_NOTIFICATION) {
						BitmapNotify n = BitmapNotify
								.newBuilder()
								.setBitmap(
										ByteString
												.copyFrom(bitsToByteArray(notification)))
								.build();

						m.setType(ClientMessage.Type.BITMAP_NOTIFY)
								.setBitmapNotify(n).setSeq(sequencer.getSeq())
								.build();

						System.out.println("Queueing BITMAP_NOTIFY for "
								+ cs.getDeviceId());

					} else {
						Notify n = Notify.newBuilder().build();

						m.setType(ClientMessage.Type.NOTIFY).setNotify(n)
								.setSeq(sequencer.getSeq()).build();
					}

					cs.getSyncScheduler().schedule(m.build(),
							sub.delayTolerance);
				}
			}
		}
	}

	public static byte[] bitsToByteArray(BitSet bits) {
		byte[] bytes = new byte[(bits.length() + 7) / 8];
		for (int i = 0; i < bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
			}
		}
		return bytes;
	}

	// Returns a bitset containing the values in bytes.
	public static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}
}
