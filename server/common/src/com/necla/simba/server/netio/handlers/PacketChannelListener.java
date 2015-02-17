/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

import java.nio.ByteBuffer;

import com.necla.simba.server.netio.handlers.PacketChannel;

/**
 * Callback interface for receiving events from a Connector. 
 * 
 * @author Nuno Santos
 */
public interface PacketChannelListener {
  /**
   * Called when a packet is fully reassembled.
   * 
   * @param pc The source of the event.
   * @param pckt The reassembled packet
   */
  public void packetArrived(PacketChannel pc, ByteBuffer pckt);
  
  /**
   * Called after finishing sending a packet.
   * 
   * @param pc The source of the event.
   * @param pckt The packet sent
   */
  public void packetSent(PacketChannel pc, ByteBuffer pckt);
  
  /**
   * Called when some error occurs while reading or writing to 
   * the socket.
   *  
   * @param pc The source of the event.
   * @param ex The exception representing the error.
   */
  public void socketException(PacketChannel pc, Exception ex);
    
  /**
   * Called when the read operation reaches the end of stream. This
   * means that the socket was closed.
   * 
   * @param pc The source of the event.
   */
  public void socketDisconnected(PacketChannel pc);
}
