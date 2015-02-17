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
package com.necla.simba.clientlib;

import java.util.ArrayList;
import java.util.List;

import android.database.Cursor;

public class SCSCursor {
	private Cursor cursor;
	private List<SCSInputStream> mis;

	public SCSCursor(Cursor cursor, List<SCSInputStream> mis) {
		this.cursor = cursor;
		this.mis = mis;
	}

	public List<SCSInputStream> getInputStream() {
		// first column in cursor is always row id!
		String row_id = cursor.getString(0);

		// create new SCSInputStream for a single row
		List<SCSInputStream> ret = new ArrayList<SCSInputStream>();
		for (SCSInputStream m : mis) {
			if (row_id.equals(m.row_id)) {
				ret.add(m);
			}
		}

		return ret;
	}

	public void close() {
		this.cursor.close();
	}

	public int getColumnCount() {
		return this.cursor.getColumnCount();
	}

	public String getColumnName(int index) {
		return this.cursor.getColumnName(index);
	}

	public int getCount() {
		return this.cursor.getCount();
	}

	public int getInt(int index) {
		return this.cursor.getInt(index);
	}

	public String getString(int index) {
		return this.cursor.getString(index);
	}

	public Long getLong(int index) {
		return this.cursor.getLong(index);
	}

	public boolean moveToFirst() {
		return this.cursor.moveToFirst();
	}

	public boolean moveToNext() {
		return this.cursor.moveToNext();
	}
}
