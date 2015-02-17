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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.gateway.server.backend.BackendFrameDecoder;

public class BackendFrameDecoder extends ByteToMessageDecoder {
	private static final Logger LOG = LoggerFactory.getLogger(BackendFrameDecoder.class);
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws Exception {

		if (in.readableBytes() < 4) {
			// length not received yet, return without producing
			// an output
			return;
		}
		
		
		// get calls on ByteBuf don't change the stream
		// so we leave 'ByteBuf in' unchanged after reading
		// the length
		int readerIndex = in.readerIndex();
		int length = in.getInt(readerIndex);
		LOG.debug("got message len=" + length + " readablebytes=" + in.readableBytes());
		
		if (in.readableBytes() < length + 4)
			return;
		
			ByteBuf frame = extractMessage(ctx, in, 4 + readerIndex, length);
			out.add(frame);
			
		in.readerIndex(readerIndex + 4 + length);
		return;
		
	}
	private ByteBuf extractMessage(ChannelHandlerContext ctx, ByteBuf in, int index, int length) {
		ByteBuf frame = ctx.alloc().buffer(length);
		frame.writeBytes(in, index, length);
		return frame;
	}
}
