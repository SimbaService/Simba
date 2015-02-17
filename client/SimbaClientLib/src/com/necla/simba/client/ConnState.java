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


public enum ConnState implements Parcelable {
	WIFI(3), FG(2), TG(1), NONE(0);
	private int value;
	
	private ConnState(int value) {
		this.setValue(value);
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	@Override
	public int describeContents() {
		return 0;
	}
	
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeString(name());
	}

	public static final Creator<ConnState> CREATOR = new Creator<ConnState>() {
		public ConnState createFromParcel(Parcel source) {
			return ConnState.valueOf(source.readString());
		}

		public ConnState[] newArray(int size) {
			return new ConnState[size];
		}
	};

}

