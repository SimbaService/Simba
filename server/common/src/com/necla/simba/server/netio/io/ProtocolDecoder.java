/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Defines the interface that should be implemented to support 
 * different protocols. 
 * 
 * The PacketChannel class is protocol-agnostic: it knows only how to read 
 * and write raw bytes from and to the underlying socket. This was done to
 * make the class general, allowing it to be used in any situation. In 
 * order to transform the raw bytes into packets, a strategy design pattern 
 * is used. PacketChannel instances have an associated ProtocolDecoder, which 
 * is used to process the incoming data and transform it into discreet packets 
 * of the corresponding protocol. It's the ProtocolDecoder instance that 
 * contains all knowledge about the protocol used in the connection. 
 *  
 * @author Nuno Santos
 */
public interface ProtocolDecoder {

  /**
   * Uses the data in the buffer given as argument to reassemble a packet.
   * 
   * The buffer contains an arbitrary amount of data (typically, all the data 
   * that was available on the read buffer of the socket). 
   * This data may or may not include a full packet. Implementations of this
   * method must be able to handle with both situations gracefully, by reading
   * from the buffer until one of the following happens: 
   *  
   * <ul>
   * <li> A full packet is reassembled. 
   * <li> No more data is available in the buffer.
   * </ul>
   * 
   * If a full packet is reassembled, that packet should be returned and
   * the buffer position should be left in the position after the end of the 
   * packet. If there is not enough data in the buffer to reassemble
   * a packet, this method should store internally the partially reassembled 
   * packet and return null. The next time it is called, it should continue 
   * reassembling the packet.
   * 
   * @param bBuffer The buffer containing the newly received data. 
   * @return null if there is not enough data in the buffer to reassemble
   * the packet or a full packet if it was possible to reassemble one.
   * 
   * @throws IOException If it is not possible to reassemble the packet 
   * because of errors in it. 
   */
  public ByteBuffer decode(ByteBuffer bBuffer) throws IOException;
}