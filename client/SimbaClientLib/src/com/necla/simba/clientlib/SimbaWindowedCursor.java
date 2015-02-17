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

import android.database.AbstractWindowedCursor;
import android.database.CursorWindow;

/***
 * Customized cursor that stores data in CursorWindow.
 * 
 * @file SimbaWindowedCursor.java
 * @author shao@nec-labs.com
 * @created 11:06:19 PM, Sep 25, 2012
 * @modified 11:06:19 PM, Sep 25, 2012
 */
public class SimbaWindowedCursor extends AbstractWindowedCursor {
	
	private String[] columnNames;

	public SimbaWindowedCursor(CursorWindow cw, String[] columnNames) {
		super();
		super.setWindow(cw);
		this.columnNames = columnNames;
	}

	@Override
	public String[] getColumnNames() {
		return columnNames;
	}

	@Override
	public int getCount() {
		return super.mWindow.getNumRows();
	}

}
