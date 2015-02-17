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

import java.util.zip.Deflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.gateway.server.frontend.FrontendFrameEncoder;
import com.necla.simba.server.netio.Stats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class FrontendFrameEncoder extends MessageToByteEncoder<ByteBuf> {
	private static final Logger LOG = LoggerFactory.getLogger(FrontendFrameEncoder.class);


	public static boolean DOCOMPRESS = true;
	private final Deflater deflater = new Deflater(); 
	private final byte[] encodeBuf = new byte[8192];

	
	@Override
	protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
			throws Exception {
		LOG.debug("told to send out msg numBytes=" + msg.readableBytes() + " to " + ctx.channel());
		int length = msg.readableBytes();
		if (DOCOMPRESS) {
			compress(ctx, msg, out);
			
		} else {
			length &= ~(1 << 30);
			out.writeInt(length);
			out.writeBytes(msg, msg.readerIndex(), msg.readableBytes());
			
			Stats.sent(length);
		}
		
		
	}
	
	private void compress(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) {
		LOG.debug("compress here");
		byte[] inAry = new byte[msg.readableBytes()];
		msg.readBytes(inAry);
		int sizeEstimate = (int) Math.ceil(inAry.length * 1.001) + 12 + 4;
		LOG.debug("compress here2");

		out.ensureWritable(sizeEstimate);

		int beginIndex = out.writerIndex();

		out.writerIndex(beginIndex + 4);

		try {

			deflater.setInput(inAry);

			while (!deflater.needsInput()) {
				LOG.debug("compress here3333");

				int numBytes = deflater.deflate(encodeBuf, 0, encodeBuf.length);
				LOG.debug("Compressed numBytes=" + numBytes);
				out.writeBytes(encodeBuf, 0, numBytes);
				LOG.debug("compress here4");

			}

			deflater.finish();
			while (!deflater.finished()) {
				int numBytes = deflater.deflate(encodeBuf, 0, encodeBuf.length);
				out.writeBytes(encodeBuf, 0, numBytes);
				LOG.debug("compress here5");

			}
			deflater.reset();
			int len = out.writerIndex() - beginIndex - 4;
			
			Stats.sent(out.writerIndex() + beginIndex + 4);
			
			LOG.debug("Compressed len=" + len);
			len |= (1 << 30);
			out.setInt(beginIndex, len);
		} catch (Exception e) {
			LOG.debug("Exception" + e);
		}
	}

}
