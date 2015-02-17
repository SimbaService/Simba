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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.protocol.Server.SimbaMessage;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.util.ConsistentHash;
import com.necla.simba.util.StatsCollector;


public class BackendConnector {
	private static final Logger LOG = LoggerFactory.getLogger(BackendConnector.class);
	
	private Map<String, Channel> backends = new HashMap<String, Channel>();
	private ConsistentHash hasher;
	private StatsCollector stats;
	private SubscriptionManager subscriptionManager;
	private ClientAuthenticationManager cam;

	class BackendConnectorIntializer extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			LOG.debug("initChannel called ch=" + ch);
			ChannelPipeline pipeline = ch.pipeline();
			//pipeline.addLast("logger",  new LoggingHandler(LogLevel.DEBUG));
			pipeline.addLast(new BackendFrameDecoder());
			pipeline.addLast(new ProtobufDecoder(SimbaMessage.getDefaultInstance()));
			pipeline.addLast(new BackendHandler(stats, subscriptionManager, cam, BackendConnector.this));
			
			pipeline.addLast(new LengthFieldPrepender(4));
			pipeline.addLast(new ProtobufEncoder());
			
		} 
		
	}
	
	// XXX: this map of channels is not thread-safe!
	// netty's DefaultChannelGroup is thread safe, but it doesn't let us do hashing
	
	public BackendConnector(StatsCollector stats)  {
		this.stats = stats;
	}
	
	public void connect(Properties props, List<String> backends, SubscriptionManager subscriptionManager, ClientAuthenticationManager cam) throws URISyntaxException, IOException {
		this.subscriptionManager = subscriptionManager;
		this.cam = cam;
		hasher = new ConsistentHash(backends);
		
		int nthreads = Integer.parseInt(props.getProperty("backend.server.thread_count", "-1"));

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(nthreads == -1 ? new NioEventLoopGroup() : new NioEventLoopGroup(nthreads))
			.handler(new BackendConnectorIntializer())
			.channel(NioSocketChannel.class);
		try {
			for (String s: backends) {
				URI uri = new URI("my://" + s);
				if (uri.getHost() == null || uri.getPort() == -1)
					throw new URISyntaxException(s, "Both host and port must be specified for host");
				LOG.debug("Connecting to: " + uri.getHost() + ":" + uri.getPort());

				ChannelFuture cf = bootstrap.connect(uri.getHost(), uri.getPort()).sync();
				if (cf.isSuccess())
					this.backends.put(s, cf.channel());
				else
					throw new IOException("Could not connect to backend " + s + ":" + uri.getPort());

			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void sendTo(String tableName, SimbaMessage msg) throws IOException {
		LOG.debug("Writing for table=" + tableName + " msgType=" + msg.getType() + " seq=" + msg.getSeq());
		String node = hasher.getNode(tableName);
		Channel ch = backends.get(node);
		LOG.debug("channel=" + ch);
		ch.writeAndFlush(msg);
        LOG.debug("finished writing seq=" + msg.getSeq());
		
	}
	
	public void send(SimbaMessage msg, Channel channel) {
		channel.write(msg);
	}


	

}
