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

import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import android.os.Handler;
import android.os.Message;
import android.os.RemoteException;
import android.util.Log;

/***
 * Utility class that manages the <client, callback> table.
 */
public class ClientCallbackManager {
	private static String TAG = "ClientCallbackManager";
	private static final ConcurrentHashMap<String, ISCSClient> client_callbacks = new ConcurrentHashMap<String, ISCSClient>();
	private static Timer timer = new Timer();
	private static boolean isTimerRunning = false;

	public synchronized static boolean clientExists(String uid) {
		return client_callbacks.containsKey(uid);
	}

	public synchronized static void addClient(String uid, ISCSClient callback,
			final Handler handler) {
		// start ping timer
		if (!isTimerRunning) {
			isTimerRunning = true;
			TimerTask t = new TimerTask() {
				@Override
				public void run() {
					synchronized (this) {
						for (ConcurrentHashMap.Entry<String, ISCSClient> entry : client_callbacks
								.entrySet()) {
							String app = entry.getKey();
							ISCSClient client = entry.getValue();
							try {
								client.ping();
							} catch (RemoteException e) {
								Log.d(TAG, "Client " + app
										+ " is not responding to ping!");
								Message m = handler
										.obtainMessage(InternalMessages.CLIENT_LOST);
								m.obj = app;
								handler.sendMessage(m);
							}
						}
					}
				}
			};
			int period = 10000; // 10 sec
			long now = System.currentTimeMillis();
			now = ((now + period - 1) / period) * period;
			timer.scheduleAtFixedRate(t, new Date(now), period);
		}
		client_callbacks.put(uid, callback);
	}

	public synchronized static ISCSClient getCallback(String uid) {
		return client_callbacks.get(uid);
	}

	public synchronized static void removeClient(String uid) {
		client_callbacks.remove(uid);
	}
}
