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
package com.necla.simba.server.gateway.server.frontend;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.protocol.Common.CreateTable;
import com.necla.simba.protocol.Common.DropTable;
import com.necla.simba.protocol.Common.NotificationPull;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.protocol.Common.PullData;
import com.necla.simba.protocol.Common.SubscribeTable;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.TornRowRequest;
import com.necla.simba.protocol.Common.UnsubscribeTable;
import com.necla.simba.protocol.Client.ControlResponse;
import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.protocol.Client.ClientMessage;
import com.necla.simba.protocol.Client.Reconnect;
import com.necla.simba.protocol.Client.RegisterDevice;
import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.gateway.server.frontend.FrontendHandler;
import com.necla.simba.server.gateway.client.ClientState;
import com.necla.simba.server.gateway.client.auth.AuthFailureException;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.client.auth.InvalidTokenException;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.util.StatsCollector;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.Channel;

public class FrontendHandler extends
		SimpleChannelInboundHandler<ClientMultiMessage> {
	private static final Logger LOG = LoggerFactory
			.getLogger(FrontendHandler.class);

	private StatsCollector stats;
	private SubscriptionManager subscriptionManager;
	private ClientAuthenticationManager cam;
	private BackendConnector backend;

	public FrontendHandler(BackendConnector backend, StatsCollector stats,
			SubscriptionManager subscriptionManager,
			ClientAuthenticationManager cam) {
		this.backend = backend;
		this.stats = stats;
		this.subscriptionManager = subscriptionManager;
		this.cam = cam;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx,
			ClientMultiMessage multimsg) throws Exception {
		ClientMessage.Builder reply;

		List<ClientMessage> msgs = multimsg.getMessagesList();
		for (ClientMessage mmsg : msgs) {
			LOG.debug("Received msg type=" + mmsg.getType() + " seq="
					+ mmsg.getSeq());
			assert stats != null : "stats must be there";
//			stats.add(mmsg.getSeq(), false);
//			stats.write(mmsg.getSeq() + " IN " + System.nanoTime());

			reply = null;
			ControlResponse crsp = null;
			ClientState cs = null;

			switch (mmsg.getType().getNumber()) {

			case ClientMessage.Type.REG_DEV_VALUE: {
				RegisterDevice crq = mmsg.getRegisterDevice();
				crsp = this.process_dev_reg(crq, ctx.channel());
			}
				break;

			case ClientMessage.Type.CREATE_TABLE_VALUE: {
				if (mmsg.hasCreateTable()) {
					CreateTable req = mmsg.getCreateTable();

					SimbaMessage msg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.CREATE_TBL)
							.setCreateTable(req).setToken(mmsg.getToken())
							.setSeq(mmsg.getSeq()).build();

					LOG.debug("handleCreateTable seq=" + mmsg.getSeq());
					backend.sendTo(req.getApp() + "." + req.getTbl(), msg);
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.SUB_TBL_VALUE: {
				if (mmsg.hasSubscribeTable()) {
					SubscribeTable crq = mmsg.getSubscribeTable();
					// crsp = this.process_tbl_sub(crq, mmsg.getToken());
					process_tbl_sub(crq, mmsg.getToken(), mmsg.getSeq());
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.RECONN_VALUE: {
				if (mmsg.hasReconnect()) {
					Reconnect crq = mmsg.getReconnect();
					LOG.debug("Got reconn req token=" + mmsg.getToken());

					try {
						cs = cam.getClientState(mmsg.getToken());
						cs.updatePacketChannel(ctx.channel());

						crsp = buildResponse(true, "ok");
					} catch (InvalidTokenException e) {
						LOG.debug("reconn invalid token");

						crsp = buildResponse(false, e.getError());
					}
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.UNSUB_TBL_VALUE: {
				if (mmsg.hasUnsubscribeTable()) {
					UnsubscribeTable crq = mmsg.getUnsubscribeTable();
					crsp = this.process_tbl_unsub(crq, mmsg.getToken());
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.DROP_TABLE_VALUE: {
				if (mmsg.hasDropTable()) {
					DropTable req = mmsg.getDropTable();

					SimbaMessage msg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.DROP_TBL)
							.setDropTable(req).setToken(mmsg.getToken())
							.setSeq(mmsg.getSeq()).build();

					backend.sendTo(req.getApp() + "." + req.getTbl(), msg);
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.UNREG_DEV_VALUE:
				// do nothing?
				break;

			case ClientMessage.Type.SYNC_REQUEST_VALUE: {
				if (mmsg.hasSyncRequest()) {
					try {
						cs = cam.getClientState(mmsg.getToken());
						SyncHeader req = mmsg.getSyncRequest().getData();
						// add transaction to map
						cs.addTransaction(req.getTransId(), req.getApp() + "."
								+ req.getTbl());
						SimbaMessage msg = SimbaMessage.newBuilder()
								.setType(SimbaMessage.Type.SYNC_REQUEST)
								.setSyncRequest(mmsg.getSyncRequest())
								.setToken(mmsg.getToken())
								.setSeq(mmsg.getSeq()).build();
						backend.sendTo(req.getApp() + "." + req.getTbl(), msg);
					} catch (InvalidTokenException e) {
						LOG.error("token invalid");
						crsp = buildResponse(false, e.getError());
					}
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.PULL_DATA_VALUE: {
				if (mmsg.hasPullData()) {
					PullData p = mmsg.getPullData();

					SimbaMessage msg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.PULL_DATA)
							.setPullData(p).setToken(mmsg.getToken())
							.setSeq(mmsg.getSeq()).build();

					try {
						SyncHeader hdr = mmsg.getPullData().getData();
						backend.sendTo(hdr.getApp() + "." + hdr.getTbl(), msg);
					} catch (IOException e) {
						e.printStackTrace();
					}
					// }
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.TORN_REQUEST_VALUE: {
				if (mmsg.hasTornRowRequest()) {
					TornRowRequest trr = mmsg.getTornRowRequest();
			
					SimbaMessage msg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.TORN_REQUEST)
							.setTornRowRequest(trr).setToken(mmsg.getToken())
							.setSeq(mmsg.getSeq()).build();

					backend.sendTo(trr.getApp() + "." + trr.getTbl(), msg);
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.OBJECT_FRAGMENT_VALUE: {
				if (mmsg.hasObjectFragment()) {
					ObjectFragment f = mmsg.getObjectFragment();

					SimbaMessage msg = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
							.setObjectFragment(f).setToken(mmsg.getToken())
							.setSeq(mmsg.getSeq()).build();

					try {

						cs = cam.getClientState(mmsg.getToken());
						String table = cs.getTransaction(f.getTransId());
						LOG.debug("Sending fragment for (" + f.getTransId()
								+ "," + f.getOid() + ", eof=" + f.getEof()
								+ ")");
						backend.sendTo(table, msg);

					} catch (InvalidTokenException e) {
						LOG.error("invalid token");
						crsp = buildResponse(false, e.getError());
					}
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			case ClientMessage.Type.NOTIFICATION_PULL_VALUE: {
				if (mmsg.hasNotificationPull()) {
					
					NotificationPull from_client = mmsg.getNotificationPull();

					if (!from_client.hasToVersion()) {
						// hack to bypass toVersion if client supplied it
						int toVersion = subscriptionManager
								.getTableVersion(from_client.getApp() + "."
										+ from_client.getTbl());

						if (from_client.getFromVersion() == toVersion) {
							// nothing new at simbastore
							// send empty reply to client
							LOG.debug("Notification Pull: fromVersion("
									+ from_client.getFromVersion()
									+ ") == toVersion(" + toVersion + ")");
							LOG.debug("sending empty PullData response");

							try {
								cs = cam.getClientState(mmsg.getToken());
							} catch (InvalidTokenException e) {
								LOG.error("invalid token");
								e.printStackTrace();
							}

							SyncHeader header = SyncHeader.newBuilder()
									.setApp(from_client.getApp())
									.setTbl(from_client.getTbl())
									.setTransId(mmsg.getSeq()).build();

							// reply with empty pull data
							PullData pull = PullData.newBuilder()
									.setData(header).build();

							ClientMessage msg = ClientMessage.newBuilder()
									.setType(ClientMessage.Type.PULL_DATA)
									.setPullData(pull)
									.setToken(mmsg.getToken())
									.setSeq(mmsg.getSeq()).build();

							cs.sendMessage(msg);

						} else if (from_client.getFromVersion() > toVersion) {
							// log error condition. client cannot pull for newer
							// version than exists
							LOG.debug("Notification Pull: fromVersion("
									+ from_client.getFromVersion()
									+ ") > toVersion(" + toVersion + ")");
							crsp = buildResponse(false, "fromVersion cannot be greater than toVersion");
						} else {
							// normal case: gateway sets toVersion
							NotificationPull to_simba = NotificationPull
									.newBuilder()
									.setApp(from_client.getApp())
									.setTbl(from_client.getTbl())
									.setFromVersion(
											from_client.getFromVersion())
									.setToVersion(toVersion).build();

							SimbaMessage msg = SimbaMessage.newBuilder()
									.setType(SimbaMessage.Type.NOTIFY_PULL)
									.setNotifypull(to_simba)
									.setToken(mmsg.getToken())
									.setSeq(mmsg.getSeq()).build();

							NotificationPull pull = msg.getNotifypull();
							backend.sendTo(pull.getApp() + "." + pull.getTbl(),
									msg);

						}
					} else {
						// hack to allow testing where client sets
						// toVersion
						SimbaMessage msg = SimbaMessage.newBuilder()
								.setType(SimbaMessage.Type.NOTIFY_PULL)
								.setNotifypull(from_client)
								.setToken(mmsg.getToken())
								.setSeq(mmsg.getSeq()).build();

						NotificationPull pull = msg.getNotifypull();
						backend.sendTo(pull.getApp() + "." + pull.getTbl(), msg);

					}
				} else {
					crsp = buildResponse(false,
							"message does not contain correct parameters");
				}
			}
				break;

			// end of switch statement
			}

			if (crsp != null) {
				assert reply == null : "Must have either a ControlResponse or another type of response";
				reply = ClientMessage.newBuilder().setSeq(mmsg.getSeq())
						.setType(ClientMessage.Type.CONTROL_RESPONSE)
						.setControlResponse(crsp);
			}

			if (reply == null)
				continue;

			// Return to sender

			try {
				if (cs != null) {
					cs.getSyncScheduler().schedule(reply.build(), 2000);
				} else {
					ClientState.sendMessage(ctx.channel(), reply.build());

				}
			} catch (IOException e) {
				LOG.error("Exception while sending to " + ctx.channel() + ": "
						+ e);
			}
		} // end for
		LOG.debug("Returning from channelRead0");
	}

	private static ControlResponse buildResponse(boolean status, String msg) {
		return ControlResponse.newBuilder().setStatus(status).setMsg(msg)
				.build();
	}

	private ControlResponse process_dev_reg(RegisterDevice req, Channel pc) {

		LOG.debug("Got <" + req.getUserId() + "," + req.getPassword()
				+ "> from " + req.getDeviceId() + "...");

		try {
			ClientState cs = cam.authenticate(req.getDeviceId(), pc,
					req.getUserId(), req.getPassword());
			return buildResponse(true, cs.getToken());
		} catch (AuthFailureException e) {
			return buildResponse(false, e.getError());

		}

	}

	private void process_tbl_sub(SubscribeTable sub, String token, int seq) {

		ClientState cs = null;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			LOG.error(e.getError());
		}

		LOG.debug("SUBSCRIBE TABLE: " + sub.getTbl() + ", SEQ=" + seq);

		cs.addSubscription(sub.getApp(), sub.getTbl(), sub.getPeriod(),
				sub.getDelayTolerance(), false, sub.getRev(), seq);
	}

	private ControlResponse process_tbl_unsub(UnsubscribeTable req, String token) {

		ClientState cs;
		try {
			cs = cam.getClientState(token);
		} catch (InvalidTokenException e) {
			return buildResponse(false, e.getError());
		}

		cs.removeSubscription(req.getApp(), req.getTbl());
		return buildResponse(true, "ok");
	}

	private static String stringify(SyncHeader sd) {
		StringBuffer sb = new StringBuffer();
		sb.append(SyncHeader.class.getCanonicalName());
		sb.append(" tbl = ").append(sd.getTbl()).append(" dirtyRows.size = ")
				.append(sd.getDirtyRowsList().size())
				.append(" deletedRows.size = ")
				.append(sd.getDeletedRowsList().size()).append(")");

		return sb.toString();
	}

	private static int fakeSeq = 314159;

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		LOG.error("exception during frontend processing");
		cause.printStackTrace();

	}
}
