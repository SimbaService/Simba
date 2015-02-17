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

import java.util.HashMap;
import java.util.Map;

import android.content.ContentValues;

/* YGO: SyncSet */
public class SyncSet {
	private String row_id;
	private ContentValues cv;
	private boolean isDelete;
	private Map<Long, Integer> objLen;
	private boolean isUpdated;

	public SyncSet(String row_id, ContentValues cv,
			boolean isDelete) {
		this.row_id = row_id;
		this.cv = cv;
		this.isDelete = isDelete;
		this.objLen = new HashMap<Long, Integer>();
		this.isUpdated = false;
	}
	
	public void setUpdated() {
		isUpdated = true;
	}
	
	public boolean isUpdated() {
		return isUpdated;
	}

	public String getRowId() {
		return row_id;
	}

	public ContentValues getContentValues() {
		return cv;
	}

	public boolean isDelete() {
		return isDelete;
	}
	
	public void setContentValues(ContentValues cv) {
		this.cv = cv;
	}
	
	public void setObjLen(Long obj_id, int length) {
		objLen.put(obj_id, length);
	}
	
	public int getObjLen(Long obj_id) {
		if (objLen.containsKey(obj_id)) {
			return objLen.get(obj_id);
		}
		return -1;
	}
}
