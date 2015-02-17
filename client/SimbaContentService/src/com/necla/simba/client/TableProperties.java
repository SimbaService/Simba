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


public class TableProperties implements Parcelable {

    private byte partial;

	
	public TableProperties(boolean partial) {
        this.partial = partial ? (byte)1 : (byte)0;
	}
	public boolean isPartial() {
		return this.partial != 0;
	}
	
    @Override
	public int describeContents() {
		return 0;
	}
	
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeByte(partial);
	}

	public static final Creator<TableProperties> CREATOR = new Creator<TableProperties>() {
		public TableProperties createFromParcel(Parcel source) {
			byte b = source.readByte();
			return new TableProperties(b != 0);
		}

		public TableProperties[] newArray(int size) {
			return new TableProperties[size];
		}
	};

}

