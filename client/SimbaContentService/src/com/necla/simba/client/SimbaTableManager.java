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
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import android.util.Log;

/***
 * Utility class that maintains the list of all SimbaTables. Others can query
 * about the table.
 */
public class SimbaTableManager {
	private static ConcurrentHashMap<String, ConcurrentHashMap<String, SimbaTable>> client_mtbl_map = new ConcurrentHashMap<String, ConcurrentHashMap<String, SimbaTable>>();
	private static Set<SimbaTable> partialTables = Collections
			.synchronizedSet(new HashSet<SimbaTable>());

	public static boolean addTable(String app_id, String tbl_id,
			SimbaTable mtbl) {
		boolean ret = false;
		ConcurrentHashMap<String, SimbaTable> app_tbls = client_mtbl_map
				.containsKey(app_id) ? client_mtbl_map.get(app_id)
				: new ConcurrentHashMap<String, SimbaTable>();
		if (!app_tbls.containsKey(tbl_id)) {
			app_tbls.put(tbl_id, mtbl);
			if (mtbl.isPartial())
				partialTables.add(mtbl);
			client_mtbl_map.put(app_id, app_tbls);
			ret = true;
		} else {
			/* table already exists */
		}

		return ret;
	}

	public static SimbaTable getTable(String app_id, String tbl_id) {
		SimbaTable ret = null;

		if (client_mtbl_map.containsKey(app_id)) {
			ConcurrentHashMap<String, SimbaTable> tbls = client_mtbl_map
					.get(app_id);
			if (tbls.containsKey(tbl_id)) {
				ret = tbls.get(tbl_id);
			} else {
				/* table name does not exist */
			}
		} else {
			/* client does not have any table */
		}
		return ret;
	}

	public static ConcurrentHashMap<String, SimbaTable> getAllTables(
			String app_id) {
		ConcurrentHashMap<String, SimbaTable> ret = new ConcurrentHashMap<String, SimbaTable>();

		if (client_mtbl_map.containsKey(app_id))
			ret = client_mtbl_map.get(app_id);

		return ret;
	}

	public static void dropTables(String app_id) {
		client_mtbl_map.remove(app_id);

	}

	public static void addRecoveredTables(String app_id, List<SimbaTable> mtbls) {
		ConcurrentHashMap<String, SimbaTable> tables = new ConcurrentHashMap<String, SimbaTable>();
		for (SimbaTable t : mtbls) 
			tables.put(t.getTblId(), t);
		client_mtbl_map.put(app_id, tables);

	}

	public static String dump() {
		int t_cnt = 0;
		StringBuilder sb = new StringBuilder();
		for (Iterator<String> iter = client_mtbl_map.keySet().iterator(); iter
				.hasNext();) {

			String app = iter.next();
			ConcurrentHashMap<String, SimbaTable> mtbls = client_mtbl_map
					.get(app);
			t_cnt += mtbls.size();
			sb.append(mtbls + "\n");
		}

		return "{SimbaTableManager: " + client_mtbl_map.size() + " apps, "
				+ t_cnt + " tables.\n" + sb.toString() + "}";
	}

	public static void trimPartials() {
		ArrayList<SimbaTable> p = new ArrayList<SimbaTable>(
				partialTables.size());
		try {
			p.addAll(partialTables);
		} catch (ConcurrentModificationException e) {
			// ignore message, unfortunately we were trimming while changing
			// state of tables
			// we'll just do the trim next time around
		}

		if (!p.isEmpty()) {
			new Thread(new TrimTask(p)).start();
		}

	}

	private static class TrimTask implements Runnable {

		private List<SimbaTable> toTrim;

		public TrimTask(List<SimbaTable> toTrim) {
			this.toTrim = toTrim;
		}

		@Override
		public void run() {
			for (SimbaTable t : toTrim)
				t.trim();

		}

	}
}
