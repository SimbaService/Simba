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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.client.SeqNumManager;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.subscription.Subscription;
import com.necla.simba.util.Pair;

public class NotificationManager {
	private static final Logger LOG = LoggerFactory
			.getLogger(NotificationManager.class);

	class Subscriptions {
		Timer t = new Timer();
		List<Pair<Subscription, ClientState>> subs = Collections
				.synchronizedList(new ArrayList<Pair<Subscription, ClientState>>());
	}

	private Map<Integer, Subscriptions> subscriptions = Collections
			.synchronizedMap(new HashMap<Integer, Subscriptions>());

	private SeqNumManager sequencer;

	public NotificationManager(SeqNumManager sequencer) {
		this.sequencer = sequencer;
	}

	public void addTask(Subscription sub, ClientState cs) {
		Pair<Subscription, ClientState> pair = new Pair<Subscription, ClientState>(
				sub, cs);
		Subscriptions s;
		boolean newPeriod = false;

		synchronized (subscriptions) {

			if (subscriptions.containsKey(sub.period)) {
				s = subscriptions.get(sub.period);
			} else {
				s = new Subscriptions();

				subscriptions.put(sub.period, s);

				newPeriod = true;
			}
			
			synchronized (s.subs) {
				if (!s.subs.contains(pair)) {
					s.subs.add(pair);
				}
			}

			if (newPeriod) {
				NotificationTask nt = new NotificationTask(sub.period,
						this.sequencer, this);

				long now = System.currentTimeMillis();
				// round up now to the next period
				LOG.debug("Period=" + sub.period);
				now = ((now + sub.period - 1) / sub.period) * sub.period;

				s.t.scheduleAtFixedRate(nt, new Date(now), sub.period);
				LOG.debug("Created timer for period=" + sub.period);
			}
		}
		dump();
	}

	public void removeTask(String table, ClientState cs) {
		Pair<Subscription, ClientState> pair = new Pair<Subscription, ClientState>(
				new Subscription(table, 0, 0), cs);

		Iterator<Map.Entry<Integer, Subscriptions>> iter = subscriptions
				.entrySet().iterator();

		synchronized (subscriptions) {

			while (iter.hasNext()) {
				Map.Entry<Integer, Subscriptions> next = iter.next();
				// int period = next.getKey();

				boolean empty = false;
				Subscriptions s = next.getValue();

				LOG.debug("removeTask list size=" + s.subs.size());

				synchronized (s.subs) {
					s.subs.remove(pair);

					empty = s.subs.isEmpty();
				}

				LOG.debug("removeTask list size=" + s.subs.size());

				if (empty) {
					iter.remove();
					s.t.cancel();
				}
			}

		}
		dump();

	}

	public List<Pair<Subscription, ClientState>> getBucket(int period) {
		Subscriptions s = subscriptions.get(period);
		return s == null ? null : s.subs;
	}

	public List<Subscription> getTables(ClientState cs, int period) {
		List<Subscription> ret = new ArrayList<Subscription>();

		Subscriptions s = subscriptions.get(period);

		synchronized (subscriptions) {
			if (s != null) {
				for (Pair<Subscription, ClientState> pair : s.subs) {
					if (pair.getSecond().equals(cs)) {
						ret.add(pair.getFirst());
					}
				}
			}
		}

		return ret;

	}

	public void dump() {
		StringBuilder sb = new StringBuilder();
		sb.append("Timers: " + subscriptions.size() + "\n");
		System.out.println(sb.toString());
	}
}
