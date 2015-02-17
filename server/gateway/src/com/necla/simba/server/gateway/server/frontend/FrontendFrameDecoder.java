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
import java.util.LinkedList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.gateway.server.frontend.FrontendFrameDecoder;
import com.necla.simba.server.netio.Stats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.JdkZlibDecoder;

public class FrontendFrameDecoder extends ByteToMessageDecoder {
	private static final Logger LOG = LoggerFactory.getLogger(FrontendFrameDecoder.class);
	
	private final Inflater inflater = new Inflater();

	

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
		int realLength = length & ~(1 << 30);
		boolean isCompressed = (length >> 30) > 0;
		LOG.debug("got message len=" + realLength + " isCompressed=" + isCompressed + " readablebytes=" + in.readableBytes());
		
		if (in.readableBytes() < realLength + 4)
			return;
		
		if (!isCompressed) {
			ByteBuf frame = extractMessage(ctx, in, 4 + readerIndex, realLength);
			out.add(frame);
		} else {
			
			ByteBuf frame = in.slice(4 + readerIndex, realLength);
			LOG.debug("going into decompress");
			ByteBuf ret = decompress(ctx, frame);
			LOG.debug("ret readablebytes=" + ret.readableBytes());
			
			out.add(decompress(ctx, frame));
			
		}
			
		in.readerIndex(readerIndex + 4 + realLength);
		
		Stats.received(readerIndex + 4 + realLength);
		
		return;
		
	}
	
	private ByteBuf decompress(ChannelHandlerContext ctx, ByteBuf frame) throws Exception {
		int readableBytes = frame.readableBytes();
		if (frame.hasArray()) {
			inflater.setInput(frame.array(), 0, readableBytes);
		} else {
			byte[] array = new byte[frame.readableBytes()];
			frame.getBytes(0, array);
			inflater.setInput(array);
		}
		int totalLength = 0;
		List<ByteBuf> all = new LinkedList<ByteBuf>();
		int multiplier = 2;
		alldone: while (true) {

			int maxOutputLength = inflater.getRemaining() * multiplier;
			// multiplier keeps increasing, so we will keep picking
			// larger and larger buffers the more times we have to loop
			// around, i.e., the more we realize that the data was very
			// heavily compressed, the larger our buffers are going to be.
			multiplier += 1;
			ByteBuf decompressed = ctx.alloc().heapBuffer(maxOutputLength);
			while (!inflater.needsInput()) {
				byte[] outArray = decompressed.array();
				int outIndex = decompressed.arrayOffset() + decompressed.writerIndex();
				int length = outArray.length - outIndex;
				if (length == 0)
					break;
				try {
					//LOG.debug("here1");
					int outputLength = inflater.inflate(outArray, outIndex, length);
					totalLength += outputLength;
					//LOG.debug("here2");

					if (outputLength > 0)
						decompressed.writerIndex(decompressed.writerIndex() + outputLength);
				} catch (DataFormatException e) {
					throw new Exception("Could not inflate" + e.getMessage());
				}
				if (inflater.finished()) {
					all.add(decompressed);
					break alldone;
				}




			}
			all.add(decompressed);
		}
		inflater.reset();
		if (all.size() == 1)
			return all.get(0);
		else {
			ByteBuf allData = ctx.alloc().heapBuffer(totalLength);
			for (ByteBuf b: all) {
				//LOG.debug("capacity=" + allData.capacity());
				allData.writeBytes(b);
				b.release();
			}
			return allData;
		}
		
	}
	
	private ByteBuf extractMessage(ChannelHandlerContext ctx, ByteBuf in, int index, int length) {
		ByteBuf frame = ctx.alloc().buffer(length);
		frame.writeBytes(in, index, length);
		return frame;
	}

}
