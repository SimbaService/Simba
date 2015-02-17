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
 * This class represents the control message request sent by client.
 * 
 * @file ControlRequest.java
 * @author shao@nec-labs.com
 * @created 10:59:03 AM, Aug 7, 2012
 * @modified 10:59:03 AM, Aug 7, 2012
 */
public class ControlRequest implements Serializable {
	private static final long serialVersionUID = 7661185695303229860L;
	public List<String> args;

	public ControlRequest(List<String> args) {
		this.args = args;
	}

	public String toString() {
		return "{req: " + args.toString() + "}";
	}
}
