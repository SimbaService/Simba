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

import java.io.Closeable;
import java.io.IOException;
import java.util.BitSet;
import java.util.Map;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

/**
 * 
 * Helper class to maintain metadata info about tables Keeps the metadata info
 * in a table
 * 
 * @author aranya
 * 
 */
public class Metadata implements Closeable {
	private static String TAG = "Metadata";
	SQLiteDatabase db;

	Metadata(String dbPath) {

		if (Preferences.WAL) {
			db = SQLiteDatabase.openDatabase(dbPath
					+ Preferences.SIMBA_DB_NAME, null,
					SQLiteDatabase.CREATE_IF_NECESSARY
							| SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING, null);
		} else {
			db = SQLiteDatabase.openOrCreateDatabase(dbPath
					+ Preferences.SIMBA_DB_NAME, null);

			if (Preferences.PERSIST) {
				/*
				 * Setting Journal mode to Persist
				 * http://www.sqlite.org/pragma.html#pragma_journal_mode
				 */
				Log.d(TAG,
						"Setting sqlite journal mode to PERSIST for Preferences.SIMBA_DB_NAME");
				db.rawQuery("PRAGMA journal_mode=PERSIST", null);
			}
		}

		final String q = "CREATE TABLE IF NOT EXISTS "
				+ Preferences.SIMBA_METADATA_TABLE
				+ "(app VARCHAR NOT NULL, tbl VARCHAR NOT NULL, key VARCHAR NOT NULL, value VARCHAR NOT NULL)";
		Log.d(TAG,
				"Creating Preferences.SIMBA_METADATA_TABLE table with query: "
						+ q);
		db.execSQL(q);

	}

	public void put(String app, String tbl, String key, String value) {
		db.delete(Preferences.SIMBA_METADATA_TABLE,
				"app=? AND tbl=? AND key=?", new String[] { app, tbl, key });
		db.execSQL("INSERT INTO " + Preferences.SIMBA_METADATA_TABLE
				+ " VALUES ('" + app + "','" + tbl + "','" + key + "','"
				+ value + "')");
	}

	public void put(String app, String tbl, String key, int value) {
		put(app, tbl, key, Integer.toString(value));
	}

	public void put(String app, String tbl, String key, boolean value) {
		put(app, tbl, key, Boolean.toString(value));
	}

	public String getString(String app, String tbl, String key, String defValue) {
		String ret = defValue;
		Cursor c = db.query(Preferences.SIMBA_METADATA_TABLE,
				new String[] { "value" }, "app = ? AND tbl = ? AND key = ?",
				new String[] { app, tbl, key }, null, null, null);
		if (c != null) {
			if (c.getCount() > 0) {
				c.moveToFirst();
				ret = c.getString(0);
			}
			c.close();
		}

		return ret;
	}

	public int getInteger(String app, String tbl, String key, int defValue) {
		int ret = defValue;
		String s = getString(app, tbl, key, null);
		if (s != null) {
			try {
				ret = Integer.parseInt(s);
			} catch (NumberFormatException e) {

			}
		}
		return ret;
	}

	public boolean getBoolean(String app, String tbl, String key,
			boolean defValue) {
		boolean ret = defValue;
		String s = getString(app, tbl, key, null);
		if (s != null) {
			try {
				ret = Boolean.parseBoolean(s);
			} catch (Exception e) {

			}
		}
		return ret;
	}

	public void remove(String app, String tbl, String[] keys) {
		for (String k : keys)
			db.execSQL("DELETE FROM " + Preferences.SIMBA_METADATA_TABLE
					+ " WHERE app = '" + app + "' AND tbl = '" + tbl
					+ "' AND key='" + k + "'");

	}

	@Override
	public void close() throws IOException {

		if (db != null) {
			db.close();
			db = null;
		}

	}

}
