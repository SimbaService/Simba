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
package com.necla.simba.apps.notes;

import android.graphics.Bitmap;

public class StringBitmap {
	private String column;
	private Bitmap object;
	private Bitmap conflictObject;

	public StringBitmap(String column, Bitmap object, Bitmap conflict) {
		super();
		this.column = column;
		this.object = object;
		this.conflictObject = conflict;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public Bitmap getObject() {
		return object;
	}

	public void setObject(Bitmap object) {
		this.object = object;
	}

	public Bitmap getConflictObject() {
		return conflictObject;
	}

	public void setConflictObject(Bitmap conflictObject) {
		this.conflictObject = conflictObject;
	}

}
