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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;

import com.necla.simba.client.SeqNumManager;
import com.necla.simba.server.gateway.client.auth.AuthFailureException;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.client.auth.DeviceNotRegisteredException;
import com.necla.simba.server.gateway.client.auth.InvalidTokenException;
import com.necla.simba.server.netio.handlers.PacketChannel;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.client.notification.NotificationManager;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;

/***
 * This class performs client authentication, the store and lookup of client
 * socket channel, which is used to send notification from server.
 * 
 * @file TokenManager.java
 * @author shao@nec-labs.com
 * @created 1:49:25 PM, Aug 7, 2012
 * @modified 1:49:25 PM, Aug 7, 2012
 */
public class ClientAuthenticationManager {
	private static final Logger LOG = LoggerFactory
			.getLogger(ClientAuthenticationManager.class);
	private Map<String, ClientState> token_state_map = new ConcurrentHashMap<String, ClientState>();
	private Map<String, ClientState> device_state_map = new ConcurrentHashMap<String, ClientState>();
	private SubscriptionManager subscriptionManager;
	private NotificationManager nom;
	private BackendConnector client;
	private SeqNumManager seq;
	
	public ClientAuthenticationManager(SubscriptionManager subscriptionManager, NotificationManager nom, BackendConnector client, SeqNumManager seq) {
		this.subscriptionManager = subscriptionManager;
		this.nom = nom;
		this.client = client;
		this.seq = seq;
	}
	
	
	public synchronized ClientState authenticate(String deviceId, Channel pc, String user, String pass) throws AuthFailureException {
		// TODO: replace this with more sophisticated authentication scheme
		if (user.equals("user") && pass.equals("pass")) {
			String token = deviceId.toString();
			ClientState cs = device_state_map.get(deviceId);
			if (cs == null) {
				cs = new ClientState(deviceId, pc, token, subscriptionManager, nom, client, seq);
				device_state_map.put(deviceId, cs);
				//cs.requestClientSubscriptions();
			} else {
				String oldToken = cs.getToken();
				token_state_map.remove(oldToken);
				cs.update(token, pc);
			}
			token_state_map.put(token, cs);
			LOG.debug("authenticated with token " + token);
			return cs;
		} else {
			throw new AuthFailureException();
		}
	}

	
	public ClientState getClientState(String token) throws InvalidTokenException {
		ClientState cs = token_state_map.get(token);
		if (cs != null) {
			return cs;
		} else {
			throw new InvalidTokenException();
		}
	}

	public synchronized ClientState getClientStateByDevice(String device) throws DeviceNotRegisteredException {
		ClientState cs = device_state_map.get(device);
		if (cs != null) {
			return cs;
		} else {
			throw new DeviceNotRegisteredException();
		}
	}	
	public synchronized List<ClientState> getAllClients() {
		List<ClientState> ret = new ArrayList<ClientState>();
		ret.addAll(token_state_map.values());
		return ret;
	}
}
