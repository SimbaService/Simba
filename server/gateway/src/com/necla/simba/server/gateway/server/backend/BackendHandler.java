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
package com.necla.simba.server.gateway.server.backend;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.PullData;
import com.necla.simba.protocol.Common.SubscribeResponse;
import com.necla.simba.protocol.Common.SubscribeTable;
import com.necla.simba.protocol.Common.SyncResponse;
import com.necla.simba.protocol.Common.TornRowResponse;
import com.necla.simba.protocol.Client.ControlResponse;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.ClientMessage.Type;
import com.necla.simba.protocol.Server.Notification;
import com.necla.simba.protocol.Server.OperationResponse;
import com.necla.simba.protocol.Server.RestoreClientSubscriptions;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.protocol.Server.TableVersion;
import com.necla.simba.server.SeqNumManager;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.server.backend.BackendHandler;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.client.auth.DeviceNotRegisteredException;
import com.necla.simba.server.gateway.client.auth.InvalidTokenException;
import com.necla.simba.util.StatsCollector;

import io.netty.channel.Channel;

public class BackendHandler extends SimpleChannelInboundHandler<SimbaMessage> {
	private static final Logger LOG = LoggerFactory
			.getLogger(BackendHandler.class);

	private ClientAuthenticationManager cam;
	private StatsCollector stats;
	private SubscriptionManager subscriptionManager;
	private BackendConnector client;

	public BackendHandler(StatsCollector stats,
			SubscriptionManager subscriptionManager,
			ClientAuthenticationManager cam, BackendConnector client) {
		this.stats = stats;
		this.subscriptionManager = subscriptionManager;
		this.cam = cam;
		this.client = client;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, SimbaMessage msg)
			throws Exception {
		LOG.debug("Got message type=" + msg.getType());
		LOG.info("Received MSG " + msg.getType() + " FROM " + ctx.channel()
				+ " SEQ: " + msg.getSeq());
		switch (msg.getType().getNumber()) {
		case SimbaMessage.Type.SYNC_RESPONSE_VALUE:
			if (msg.hasSyncResponse()) {
				handleSyncResponse(msg.getSyncResponse(), msg.getSeq(),
						msg.getToken());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.PULL_DATA_VALUE:
			if (msg.hasPullData()) {
				handlePullData(msg.getPullData(), msg.getSeq(), msg.getToken());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.TORN_RESPONSE_VALUE:
			if (msg.hasTornRowResponse()) {
				handleTornRowResponse(msg.getTornRowResponse(), msg.getSeq(),
						msg.getToken());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.NOTIFY_VALUE:
			if (msg.hasNotify()) {
				handleNotify(msg.getNotify());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.RESTORE_CLIENT_SUBSCRIPTIONS_VALUE:
			if (msg.hasRestoreClientSubscriptions()) {
				handleRestoreClientSubscriptions(msg
						.getRestoreClientSubscriptions());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.OBJECT_FRAGMENT_VALUE:
			if (msg.hasObjectFragment()) {
				handleObjectFragment(msg.getObjectFragment(), msg.getSeq(),
						msg.getToken());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.SUB_RESPONSE_VALUE:
			if (msg.hasSubscribeResponse()) {
				handleSubscribeResponse(msg.getSubscribeResponse(),
						msg.getSeq());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.PING_VALUE:
			if (msg.hasPing()) {
				handlePing(msg, ctx.channel());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.TABLE_VERSION_VALUE:
			if (msg.hasTableVersion()) {
				handleTableVersion(msg.getTableVersion());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		case SimbaMessage.Type.OP_RESPONSE_VALUE:
			if (msg.hasOperationResponse()) {
				handleOperationResponse(msg.getOperationResponse(),
						msg.getSeq(), msg.getToken());
			} else {
				LOG.error(msg.getType()
						+ "message does not contain correct message parameters");
			}
			break;

		}
	}

	private void handleOperationResponse(OperationResponse operationResponse,
			int seq, String token) {
		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		SimbaMessage saved = SeqNumManager.getPendingMsg(seq);

		if (saved != null) {
			// if this was a SubscribeTable message, clean-up required
			if (saved.hasSubscribeTable()) {
				handleSubscribeTableCleanup(saved.getSubscribeTable(), token,
						seq);
			}
		}

		SeqNumManager.removePendingSeq(seq);

		LOG.debug("handleOperationResponse seq=" + seq);

		ControlResponse.Builder b = ControlResponse.newBuilder()
				.setStatus(operationResponse.getStatus())
				.setMsg(operationResponse.getMsg());

		ClientMessage msg = ClientMessage.newBuilder()
				.setType(Type.CONTROL_RESPONSE).setControlResponse(b)
				.setSeq(seq).setToken(token).build();

		try {
			cs.sendMessage(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void handleSubscribeTableCleanup(SubscribeTable subscribeTable,
			String token, int seq) {
		// cleanup client subscriptions
		ClientState client = null;
		try {
			client = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		client.removeSubscription(subscribeTable.getApp(),
				subscribeTable.getTbl());

		LOG.debug("Subscribe failed; table=" + subscribeTable.getApp() + "."
				+ subscribeTable.getTbl() + ", client=" + client.getDeviceId());
	}

	/**
	 * @param tableVersion
	 */
	private void handleTableVersion(TableVersion tableVersion) {
		subscriptionManager.setVersion(tableVersion.getTable(),
				tableVersion.getVersion());
	}

	/**
	 * @param ping
	 * @param seq
	 * @param token
	 */
	private void handlePing(SimbaMessage ping, Channel channel) {
		LOG.debug("Sending PING REPLY to " + channel);
		client.send(ping, channel);
	}

	/**
	 * @param subscribeResponse
	 * @param seq
	 * @param token
	 */
	private void handleSubscribeResponse(SubscribeResponse subscribeResponse,
			int seq) {

		SimbaMessage saved = SeqNumManager.getPendingMsg(seq);
		String tablename = saved.getSubscribeTable().getApp() + "."
				+ saved.getSubscribeTable().getTbl();
		subscriptionManager.addSchema(tablename, subscribeResponse, seq);
	}

	/**
	 * @param tornRowResponse
	 * @param seq
	 * @param token
	 */
	private void handleTornRowResponse(TornRowResponse tornRowResponse,
			int seq, String token) {

		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		ClientMessage msg = ClientMessage.newBuilder()
				.setType(Type.TORN_RESPONSE)
				.setTornRowResponse(tornRowResponse).setSeq(seq)
				.setToken(token).build();

		try {
			cs.sendMessage(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param objectFragment
	 */
	private void handleObjectFragment(ObjectFragment objectFragment, int seq,
			String token) {
		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		ClientMessage msg = ClientMessage.newBuilder()
				.setType(Type.OBJECT_FRAGMENT)
				.setObjectFragment(objectFragment).setSeq(seq).setToken(token)
				.build();

		try {
			cs.sendMessage(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// private void handleRestoreGatewaySubscriptions(
	// RestoreSubscriptions restoreSubscriptions) {
	// subscriptionManager.restoreSubscriptions(restoreSubscriptions);
	// }

	private void handleRestoreClientSubscriptions(
			RestoreClientSubscriptions restoreClientSubscriptions) {
		ClientState cs = null;
		try {
			cs = cam.getClientStateByDevice(restoreClientSubscriptions
					.getClientId());
		} catch (DeviceNotRegisteredException e) {
			LOG.error("No ClientState object match for device_id="
					+ restoreClientSubscriptions.getClientId());
			e.printStackTrace();
		}
		cs.restoreClientState(restoreClientSubscriptions);
	}

	private void handleNotify(Notification m) {
		subscriptionManager.processNotification(m.getTable(), m.getVersion());
	}

	private void handlePullData(PullData m, int seq, String token) {
		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		ClientMessage msg = ClientMessage.newBuilder().setType(Type.PULL_DATA)
				.setPullData(m).setSeq(seq).setToken(token).build();

		try {
			cs.sendMessage(msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void handleSyncResponse(SyncResponse m, int seq, String token) {
		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error("No ClientState object match for token=" + token);
			e.printStackTrace();
		}

		ClientMessage msg = ClientMessage.newBuilder()
				.setType(Type.SYNC_RESPONSE).setSyncResponse(m).setSeq(seq)
				.build();

		try {
			cs.sendMessage(msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		LOG.error("exception during backend processing");
		cause.printStackTrace();

	}

}
