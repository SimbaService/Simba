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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslHandler;

import java.util.Properties;

import javax.net.ssl.SSLEngine;

import com.necla.simba.protocol.Client.ClientMultiMessage;
import com.necla.simba.server.gateway.client.auth.ClientAuthenticationManager;
import com.necla.simba.server.gateway.server.backend.BackendConnector;
import com.necla.simba.server.gateway.subscription.SubscriptionManager;
import com.necla.simba.util.StatsCollector;


public class FrontendServer {
	
	private BackendConnector backend;
	private StatsCollector stats;
	private SubscriptionManager subscriptionManager;
	private ClientAuthenticationManager cam;
	
	class FrontendServerInitializer extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			ChannelPipeline pipeline = ch.pipeline();
			SSLEngine engine = SSLContextFactory.getServerContext().createSSLEngine();
			engine.setUseClientMode(false);
			engine.setNeedClientAuth(true);
			assert stats != null : "stats must be there already";
			//pipeline.addLast("logger",  new LoggingHandler(LogLevel.DEBUG));
			pipeline.addLast("sslDecoder", new SslHandler(engine));
			pipeline.addLast("frameDecoder", new FrontendFrameDecoder());
			pipeline.addLast("protobufDecoder", new ProtobufDecoder(ClientMultiMessage.getDefaultInstance()));
			pipeline.addLast("messageHandler", new FrontendHandler(backend, stats, subscriptionManager, cam));

			pipeline.addLast("frameEncoder", new FrontendFrameEncoder());
			pipeline.addLast("protobufEncoder", new ProtobufEncoder());
			
			
		}
		
	}
	
	public FrontendServer(Properties props, BackendConnector backend, StatsCollector stats, SubscriptionManager subscriptionManager, ClientAuthenticationManager cam) throws Exception {
		this.backend = backend;
		
		this.stats = stats;
		assert this.stats != null : "stats must be there already";
		this.subscriptionManager = subscriptionManager;
		this.cam = cam;
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(new NioEventLoopGroup(), new NioEventLoopGroup())
			.channel(NioServerSocketChannel.class)
			.childHandler(new FrontendServerInitializer())
			.option(ChannelOption.SO_BACKLOG, 128);
		int port = Integer.parseInt(props.getProperty("port", "9000"));
		ChannelFuture f = bootstrap.bind(port).sync();
		f.channel().closeFuture().sync();
	}
}
