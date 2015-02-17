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

/***
 * Utility class that stores constants.
 */
public class ControlMessage {
	// Control message commands: register, unregister, subscribe, unsubscribe
	public static final byte REG_DEV = 1; // register a device
	public static final byte UNREG_DEV = 2; // unregister a device
	public static final byte SUB_TBL = 3; // subscribe a table for read
	public static final byte UNSUB_TBL = 4; // unsubscribe a table for read
	public static final byte RET_STATUS = 5; // return command status
	public static final byte RECONN = 6; // re-establish the connection
}
