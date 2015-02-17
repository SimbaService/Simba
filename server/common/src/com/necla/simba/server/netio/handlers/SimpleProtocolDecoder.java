/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import com.necla.simba.server.netio.io.ProtocolDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Decoder for an imaginary protocol. The packets of this protocol have the
 * following (very simple) format:
 * 
 * <STX><Data><ETX>
 * 
 * where
 * 
 * - <STX> is ascii 02.
 * - <ETX> is ascii 03.
 * - <asciiData> is any valid character.
 * 
 * @author Nuno Santos
 */
final public class SimpleProtocolDecoder implements ProtocolDecoder {
  /** Initial byte of a packet. */
  public static final byte STX = 0x02;
  /** Final byte of a packet. */
  public static final byte ETX = 0x03;  
  
  /**
   * Size of the buffer used to reconstrut the packet. Should be big 
   * enough to hold an entire packet.
   */
  private final static int BUFFER_SIZE = 100*1024;
  
  
  /**
   * Holds a message that is not fully assembled. This buffer is fixed-size.
   * If it is exceed, an Exception is raised by the decode() method. 
   */
  private byte[] buffer = new byte[BUFFER_SIZE];
  /** Write position on the previous buffer. */
  private int pos = 0;
  
  public ByteBuffer decode(ByteBuffer socketBuffer) throws IOException {    
    // Reads until the buffer is empty or until a packet
    // is fully reassembled.
    while (socketBuffer.hasRemaining()) {
      // Copies into the temporary buffer
      byte b = socketBuffer.get();
      try {
        buffer[pos] = b;
      } catch (IndexOutOfBoundsException e) {
        // The buffer has a fixed limit. If this limit is reached, them
        // most likely the packet that is being read is corrupt.
        e.printStackTrace();
        throw new IOException(
            "Packet too big. Maximum size allowed: " + BUFFER_SIZE + " bytes.");
      }
      pos++;
      
      // Check if it is the final byte of a packet.
      if (b == ETX) {
        // The current packet is fully reassembled. Return it
        byte[] newBuffer = new byte[pos];
        System.arraycopy(buffer, 0, newBuffer, 0, pos);
        ByteBuffer packetBuffer = ByteBuffer.wrap(newBuffer);        
        pos = 0;

        return packetBuffer;
      }
    }
    // No packet was reassembled. There is not enough data. Wait
    // for more data to arrive.
    return null;
  }
}