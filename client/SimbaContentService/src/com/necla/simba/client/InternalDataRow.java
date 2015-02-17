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
import java.util.List;

import android.os.Parcel;
import android.os.Parcelable;

/***
 * This is the internal data row used between Simba client and Simba adapter,
 * which has one additional column _id.
 */
public class InternalDataRow extends DataRow implements Parcelable {
	private static final long serialVersionUID = 1L;
	private String id;
	private int isDeleted;

	public InternalDataRow(String id, List<String> columnData,
			List<Long> objectData, boolean isDeleted) {
		super(columnData, objectData);
		this.id = id;
		this.isDeleted = (isDeleted == true ? 1 : 0);
	}

	public String getId() {
		return id;
	}

	public boolean isDeleted() {
		return (isDeleted == 1);
	}

	public String toString() {
		return "{" + id + ":" + super.toString() + "}";
	}

	public int describeContents() {
		return 0;
	}

	public void writeToParcel(Parcel dest, int flags) {
		dest.writeString(id);
		dest.writeInt(isDeleted);
		dest.writeStringList(super.getColumnData());
		dest.writeList(super.getObjectData());
	}

	public static final Creator<InternalDataRow> CREATOR = new Creator<InternalDataRow>() {
		public InternalDataRow createFromParcel(Parcel source) {
			String id = source.readString();
			int isDeleted = source.readInt();
			List<String> columnData = new ArrayList<String>();
			List<Long> objectData = new ArrayList<Long>();
			source.readStringList(columnData);
			source.readList(objectData, null);

			return new InternalDataRow(id, columnData, objectData, isDeleted == 1);
		}

		public InternalDataRow[] newArray(int size) {
			return new InternalDataRow[size];
		}
	};
}
