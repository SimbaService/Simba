/*******************************************************************************
 *   Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
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
package com.necla.simba.server.gateway.client.auth;

import com.necla.simba.client.SimbaException;

/***
 * @file DeviceNotRegisteredException.java
 * @author shao@nec-labs.com
 * @created 3:27:10 PM, Aug 7, 2012
 * @modified 3:27:10 PM, Aug 7, 2012
 */
public class DeviceNotRegisteredException extends SimbaException {
	private static final long serialVersionUID = 471145507237707189L;

	public DeviceNotRegisteredException() {
		super("Device not registered.");
	}
}
