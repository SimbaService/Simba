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

import android.database.CursorWindow;
import android.os.Parcel;
import android.os.Parcelable;


public class SimbaCursorWindow implements Parcelable {

    private String[] columns;
    private CursorWindow w;
	
	public SimbaCursorWindow(String[] columns, CursorWindow w) {
        this.columns = columns;
        this.w = w;
	}

    public String[] getColumns() {
        return this.columns;
    }

    public CursorWindow getWindow() {
        return this.w;
    }
	
    @Override
	public int describeContents() {
		return 0;
	}
	
	public void writeToParcel(Parcel dest, int flags) {
		dest.writeStringArray(this.columns);
        dest.writeParcelable(w, flags);
        
	}

	public static final Creator<SimbaCursorWindow> CREATOR = new Creator<SimbaCursorWindow>() {
		public SimbaCursorWindow createFromParcel(Parcel source) {
            String[] cols = source.createStringArray();
            CursorWindow w = (CursorWindow) source.readParcelable(CursorWindow.class.getClassLoader());
			return new SimbaCursorWindow(cols, w);
		}

		public SimbaCursorWindow[] newArray(int size) {
			return new SimbaCursorWindow[size];
		}
	};

}

