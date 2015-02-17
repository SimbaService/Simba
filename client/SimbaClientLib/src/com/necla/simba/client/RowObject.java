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

import android.os.Parcel;
import android.os.Parcelable;

/**
 * @author Younghwan Go
 * @created Jul 16, 2013 1:59:41 PM
 * 
 */
public class RowObject implements Parcelable {
	private String tbl;
	private String row_id;
	private long obj_id;
	private int offset;

	public RowObject(String tbl, String row_id, long obj_id, int offset) {
		this.tbl = tbl;
		this.row_id = row_id;
		this.obj_id = obj_id;
		this.offset = offset;
	}
	
	public String GetTbl() {
		return tbl;
	}
	
	public String GetRowID() {
		return row_id;
	}

	public long GetObjectID() {
		return obj_id;
	}

	public int GetOffset() {
		return offset;
	}

	public int describeContents() {
		return 0;
	}

	public void writeToParcel(Parcel dest, int flags) {
		dest.writeString(tbl);
		dest.writeString(row_id);
		dest.writeLong(obj_id);
		dest.writeInt(offset);
	}

	public static final Parcelable.Creator<RowObject> CREATOR = new Parcelable.Creator<RowObject>() {
		public RowObject createFromParcel(Parcel in) {
			String tbl = in.readString();
			String rid = in.readString();
			long oid = in.readLong();
			int off = in.readInt();

			return new RowObject(tbl, rid, oid, off);
		}

		public RowObject[] newArray(int size) {
			return new RowObject[size];
		}
	};

}
