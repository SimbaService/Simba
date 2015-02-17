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

import java.util.HashMap;
import java.util.Map;

import com.necla.simba.protocol.Client.ClientMessage;

/***
 * Utility class that manages the sequence number and performs simple protocol
 * check. e.g. the return message will carry the sequence number of previous
 * control message.
 */
public class SeqNumManager {
	private int cnt = (int) (Math.random() * Integer.MAX_VALUE);
	private Map<Integer, ClientMessage> map = new HashMap<Integer, ClientMessage>();
	private Object lock = new Object();
	private Object maplock = new Object();

	public int getSeq() {
		synchronized (lock) {
			cnt = (int) ((cnt + 1) % Integer.MAX_VALUE);
		}
		return cnt;
	}

	public void addPendingSeq(int seq, ClientMessage mmsg) {
		synchronized (maplock) {
			ClientMessage prev = map.put(seq, mmsg);
			assert prev == null;
		}
	}

	public ClientMessage getPendingMsg(int seq) {
		synchronized (maplock) {
			return map.get(seq);
		}
	}

	public void removePendingSeq(int seq) {
		synchronized (maplock) {
			map.remove(seq);
		}
	}
}
