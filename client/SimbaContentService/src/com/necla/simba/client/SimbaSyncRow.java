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

import com.necla.simba.protocol.DataRow;

public class SimbaSyncRow {
	private DataRow row;
	private int numObj;

	public SimbaSyncRow(DataRow row, int numObj) {
		this.row = row;
		this.numObj = numObj;
	}

	public DataRow getDataRow() {
		return row;
	}

	public int getCount() {
		return numObj;
	}

	public void decrementCount() {
		numObj--;
	}
}
