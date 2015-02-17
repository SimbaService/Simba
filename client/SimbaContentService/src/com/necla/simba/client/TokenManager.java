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

import java.util.ArrayList;
import java.util.List;

import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.Reconnect;
import com.necla.simba.protocol.RegisterDevice;

import android.os.AsyncTask;
import android.util.Log;

/***
 * This class stores the token returned by server.
 */
public class TokenManager {
	private String token = null;
	private SimbaNetworkManager manager;

	public TokenManager(SimbaNetworkManager manager) {
		this.manager = manager;
	}

	public void setToken(String t) {
		token = t;
	}

	public String getToken() {
		return token;
	}

	public void networkConnected() {
		if (token != null) {
			reconnect_dev();
		} else {
			reg_dev();
		}
	}

	private void reconnect_dev() {
		new AsyncTask<Void, Void, Void>() {
			protected Void doInBackground(Void... params) {
				int seq = SeqNumManager.getSeq();
				Reconnect r = Reconnect.newBuilder().setDummy(0).build();
				SimbaMessage m = SimbaMessage.newBuilder().setSeq(seq)
						.setToken(token).setType(SimbaMessage.Type.RECONN)
						.setReconnect(r).build();

				SeqNumManager.addPendingSeq(seq, m);

				// This message must jump the line
				manager.sendFirstMessage(m);
				return null;
			}
		}.execute();

	}

	private void reg_dev() {
		new AsyncTask<Void, Void, Void>() {
			protected Void doInBackground(Void... params) {
				int seq = SeqNumManager.getSeq();

				RegisterDevice r = RegisterDevice.newBuilder()
						.setDeviceId(WalletManager.getDeviceID())
						.setUserId(WalletManager.getUserID())
						.setPassword(WalletManager.getUserPassword()).build();

				SimbaMessage m = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.REG_DEV).setSeq(seq)
						.setRegisterDevice(r).build();
				SeqNumManager.addPendingSeq(seq, m);
				manager.sendMessage(m);

				return null;
			}
		}.execute();
	}
}
