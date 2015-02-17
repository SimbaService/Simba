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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

/**
 * @author Younghwan Go
 * @created Jul 31, 2013 2:30:33 PM
 * 
 */
public class SimbaChunkList {
	private final static String TAG = "SimbaChunkList";
	static SQLiteDatabase db;
	private static Map<Long, BitSet> dirtyChunkList = new HashMap<Long, BitSet>();

	public static boolean isDirtyChunkListEmpty() {
		return dirtyChunkList.isEmpty();
	}

	public static void resetList() {
		dirtyChunkList.clear();
	}
	
	public static void storeDirtyChunkList(String app, String tbl) {
		SimbaTable mtbl = SimbaTableManager.getTable(app, tbl);

		for (Map.Entry<Long, BitSet> entry : dirtyChunkList.entrySet()) {
			ContentValues cv = new ContentValues();
			long obj_id = entry.getKey();
			BitSet bitmap = entry.getValue();
			String chunklist = null;
			for (int i = 0; i < bitmap.size(); i++) {
				if (bitmap.get(i) == true) {
					if (chunklist == null) {
						chunklist = "1";
					} else {
						chunklist += "1";
					}
				} else {
					if (chunklist == null) {
						chunklist = "0";
					} else {
						chunklist += "0";
					}
				}
			}
			assert (chunklist != null);
			cv.put("obj", obj_id);
			cv.put("chunklist", chunklist);
			Log.d(TAG, "Storing obj_id: " + obj_id + ", dirtyChunkList: '"
					+ chunklist + "' into DIRTYCHUNKLIST TABLE");
			mtbl.insertDirtyChunkList(cv);
		}
	}

	public static boolean recoverDirtyChunkList(SimbaTable mtbl,
			List<Long> torn_objs) {
		assert (dirtyChunkList.isEmpty());
		boolean hasTornRow = false;

		Cursor cursor = mtbl.getDirtyChunkList();
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				// add to dirtyChunkList if the object is not in a torn row
				if (!torn_objs.contains(cursor.getLong(0))) {
					String bitmap = cursor.getString(1);
					for (int i = 0; i < bitmap.length(); i++) {
						if (bitmap.charAt(i) == '1') {
							addDirtyChunk(cursor.getLong(0), i);
						}
					}
				} else {
					hasTornRow = true;
				}
			} while (cursor.moveToNext());

			// store new dirtyChunkList if there was any object in torn row
			if (hasTornRow) {
				mtbl.deleteDirtyChunkList();
				storeDirtyChunkList(mtbl.getAppId(), mtbl.getTblId());
			}
			Log.d(TAG, "Recovered dirtyChunkList");
			return true;
		}
		return false;
	}

	public static void removeDirtyChunkList(String app, String tbl) {
		SimbaTable mtbl = SimbaTableManager.getTable(app, tbl);
		mtbl.deleteDirtyChunkList();
		Log.d(TAG, "Removing dirtyChunkList from DIRTYCHUNKLIST TABLE <" + app
				+ ", " + tbl + ">");
	}

	public static void removeDirtyChunks(long obj_id) {
		if (dirtyChunkList.remove(obj_id) != null) {
			Log.d(TAG, "Removed obj_id: " + obj_id + " from dirtyChunkList");
		}
	}
	
	public static void removeDirtyChunk(long obj_id, int chunk_num) {
		BitSet chunks = dirtyChunkList.get(obj_id);
		if (chunks != null) {
			chunks.set(chunk_num, Boolean.FALSE);
			dirtyChunkList.put(obj_id, chunks);
		}
	}

	public static void addDirtyChunk(long obj_id, int chunk_num) {
		Log.d(TAG, "Adding <" + obj_id + ", " + chunk_num
				+ "> to dirty chunk list");

		BitSet chunks = dirtyChunkList.get(obj_id);
		if (chunks == null) {
			chunks = new BitSet();
		}

		while (chunks.size() < chunk_num) {
			BitSet old_chunks = (BitSet) chunks.clone();
			chunks = new BitSet(chunks.size() * 2);
			chunks.or(old_chunks);
		}
		chunks.set(chunk_num, Boolean.TRUE);
		dirtyChunkList.put(obj_id, chunks);
	}

	public static BitSet getDirtyChunks(long obj_id) {
		return dirtyChunkList.get(obj_id);
	}
}
