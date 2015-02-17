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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Timer;

/***
 * This class manages the list of timers associated with those tables that
 * require periodic sync.
 */
public class WriteTimerManager {
	
	
	private Hashtable<Integer, WriteTimerTask> map = new Hashtable<Integer, WriteTimerTask>();
	private Timer timer = new Timer();

	private SyncScheduler syncScheduler;
	private ConnectionState cs;

	public WriteTimerManager(SyncScheduler syncScheduler, ConnectionState cs) {
		this.syncScheduler = syncScheduler;
		this.cs = cs;
	}
	/**
	 * Add table to corresponding timer bucket.
	 * 
	 * @param tbl
	 *            table in local store
	 * @param period
	 *            interval for periodic sync
	 */
	public void addTask(SimbaTable tbl) {
		int period = tbl.getSyncPeriod(true);

		if (map.containsKey(period)) {
			WriteTimerTask t = map.get(period);
			t.addTable(tbl);
		} else {
			WriteTimerTask t = new WriteTimerTask(period, syncScheduler, cs);
			t.addTable(tbl);
			
			long now = System.currentTimeMillis();
			// round up now to the next period
			now = ((now + period - 1)/period)*period;
			map.put(period, t);			

			timer.scheduleAtFixedRate(t, new Date(now), period);			
		}		
	}

	public void removeTask(SimbaTable tbl) {		
		int period = tbl.getSyncPeriod(true);
		
		WriteTimerTask t = map.get(period);
		if (t != null) {
			boolean last = t.removeTable(tbl);
			if (last) {
				t.cancel();
				map.remove(period);
			}
		}
		tbl.disableSync(true);		
	}
	
	
	public void updateTask(SimbaTable tbl) {
		
		int period = tbl.getSyncPeriod(true);
		WriteTimerTask t = map.get(period);
		if (t != null) {
			t.removeTable(tbl);
			long now = System.currentTimeMillis();
			// round up now to the next period
			now = ((now + period - 1)/period)*period;
			t.addTable(tbl);
			map.put(period, t);
			timer.scheduleAtFixedRate(t, new Date(now), period);
		}
		
	}

	
	
/*
	public String dump() {
		StringBuilder sb = new StringBuilder();
		sb.append("Tables: " + map + "\n");
		sb.append("Timers: " + timers + "\n");
		return sb.toString();
	}
	*/
}
