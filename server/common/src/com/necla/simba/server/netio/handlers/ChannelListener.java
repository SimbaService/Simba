/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.handlers;

/**
 * @author Nuno Santos
 */
public interface ChannelListener {
  /**
   * Called when the associated socket is ready to be read.
   */
  public void handleRead();
  /**
   * Called when the associated socket is ready to be written.
   */  
  public void handleWrite();  
}
