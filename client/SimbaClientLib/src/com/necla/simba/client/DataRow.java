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

import java.io.Serializable;
import java.util.List;

/***
 * Abstract class that stores only user-defined data.
 * 
 * @file DataRow.java
 * @author shao@nec-labs.com
 * @created 5:37:31 PM, Jul 24, 2012
 * @modified 5:37:31 PM, Jul 24, 2012
 */
public abstract class DataRow implements Serializable {
	private static final long serialVersionUID = -4403303777759793631L;
	private List<String> columnData;
	private List<Long> objectData;

	public DataRow(List<String> columnData, List<Long> objectData) {
		this.columnData = columnData;
		this.objectData = objectData;
	}

	public List<String> getColumnData() {
		return columnData;
	}
	
	public List<Long> getObjectData() {
		return objectData;
	}

	public String toString() {
		return (columnData == null) ? "null" : columnData.toString();
	}
}
