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
import java.util.List;
import java.util.Map;

import android.util.Log;

import com.necla.simba.protocol.DataRow;
import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.ObjectFragment;
import com.necla.simba.protocol.ObjectHeader;
import com.necla.simba.client.SimbaSyncRow;

/**
 * @author Younghwan Go
 * @created Jul 22, 2013 4:44:08 PM
 * 
 */
public class SimbaSyncObject {
	private final String TAG = "SimbaSyncObject";
	private Map<Integer, Long> obj_list;
	private HashMap<Integer, SimbaObject> objects;
	private String app_id;
	private String tbl_id;
	private List<DataRow> dirtyRows;
	private int num_objects = 0;
	private Map<Integer, String> oidToRow = new HashMap<Integer, String>();
	private Map<String, SimbaSyncRow> objCount = new HashMap<String, SimbaSyncRow>();
	private int newRows = 0;
	private int conflictedRows = 0;
	private int numDeletedRows = 0;
	private final int opType;

	public SimbaSyncObject(String app_id, String tbl_id,
			List<DataRow> dirtyRows, Map<Integer, Long> obj_list, int operationType) {
		this.obj_list = obj_list;
		this.app_id = app_id;
		this.tbl_id = tbl_id;
		this.dirtyRows = dirtyRows;
		this.opType = operationType;

		// number of objects
		for (DataRow row : dirtyRows) {
			num_objects += row.getObj().size();

			int objNum = 0;
			List<ObjectHeader> oh_list = row.getObj();
			for (ObjectHeader oh : oh_list) {
				// if (obj_list.get(oh.getOid()) == null) {
				// break;
				// }
				oidToRow.put(oh.getOid(), row.getId());
				objNum++;
			}
			if (objNum > 0) {
				SimbaSyncRow msr = new SimbaSyncRow(row, objNum);
				objCount.put(row.getId(), msr);
			}
		}

		// hashmap for object list
		if (num_objects > 0) {
			objects = new HashMap<Integer, SimbaObject>(num_objects);
		}
	}
	
	public int getType() {
		return this.opType;
	
	}

	public String getApp() {
		return app_id;
	}

	public String getTbl() {
		return tbl_id;
	}

	public List<DataRow> getDirtyRows() {
		return dirtyRows;
	}

	public int remainingObjects() {
		return num_objects;
	}

	public void setRowCounts(int[] counts) {
		assert (counts.length == 3);
		newRows += counts[0];
		conflictedRows += counts[1];
		numDeletedRows += counts[2];
	}

	public int getNewRows() {
		return newRows;
	}

	public int getConflictedRows() {
		return conflictedRows;
	}

	public int getDeletedRows() {
		return numDeletedRows;
	}

	public int getRowRev(ObjectFragment f) {
		String row_id = oidToRow.get(f.getOid());
		SimbaSyncRow msr = objCount.get(row_id);

		oidToRow.remove(f.getOid());
		objCount.remove(row_id);

		return msr.getDataRow().getRev();
	}

	public DataRow addFragment(ObjectFragment f) {
		// check whether the object exists in obj_list,
		// if not, it means the row was duplicate and dropped
		if (obj_list.get(f.getOid()) == null) {
			Log.d(TAG, "Object oid[" + f.getOid()
					+ "] was dropped from duplicate row");
			if (f.getEof() == true) {
				num_objects--;
			}
			return null;
		}

		if (objects != null) {
			if (objects.get(f.getOid()) == null) {
				// create new SimbaObject
				long obj_id = obj_list.get(f.getOid());
				SimbaObject obj = new SimbaObject(obj_id);
				obj.add(f);
				objects.put(f.getOid(), obj);
			} else {
				objects.get(f.getOid()).add(f);
			}
			// end of object
			if (f.getEof() == true) {
				num_objects--;

				// check if the row is complete
				String row_id = oidToRow.get(f.getOid());
				SimbaSyncRow msr = objCount.get(row_id);
				msr.decrementCount();
				if (msr.getCount() == 0) {
					oidToRow.remove(f.getOid());
					objCount.remove(row_id);
					// ready to process this row
					return msr.getDataRow();
				}
			}
		}
		return null;
	}
}
