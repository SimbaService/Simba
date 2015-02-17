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
package com.necla.simba.server;

import java.util.HashMap;
import java.util.Map;

import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Server.SimbaMessage;

/***
 * Utility class that manages the sequence number and performs simple protocol
 * check. e.g. the return message will carry the sequence number of previous
 * control message.
 * 
 * @file SeqNumManager.java
 * @author shao@nec-labs.com
 * @created 5:25:03 PM, Aug 6, 2012
 * @modified 5:25:03 PM, Aug 6, 2012
 */
public class SeqNumManager {
	private static int cnt = (int) (Math.random() * Integer.MAX_VALUE);
	private static Map<Integer, SimbaMessage> map = new HashMap<Integer, SimbaMessage>();
	private static Object lock = new Object();
	private static Object maplock = new Object();

	public static int getSeq() {
		synchronized (lock) {
			cnt = (int) ((cnt + 1) % Integer.MAX_VALUE);
		}
		return cnt;
	}

	public static void addPendingSeq(int seq, SimbaMessage mmsg) {
		synchronized (maplock) {
			SimbaMessage prev = map.put(seq, mmsg);
			assert prev == null;
		}
	}

	public static SimbaMessage getPendingMsg(int seq) {
		synchronized (maplock) {
			return map.get(seq);
		}
	}

	public static void removePendingSeq(int seq) {
		synchronized (maplock) {
			map.remove(seq);
		}
	}
}
