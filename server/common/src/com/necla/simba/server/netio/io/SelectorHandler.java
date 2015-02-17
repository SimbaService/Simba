/*
 * Copyright 2004 WIT-Software, Lda. 
 * - web: http://www.wit-software.com 
 * - email: info@wit-software.com
 *
 * All rights reserved. Relased under terms of the 
 * Creative Commons' Attribution-NonCommercial-ShareAlike license.
 */
package com.necla.simba.server.netio.io;

/**
 * Marker interface for classes that are able to handle I/O events
 * raised by the SelectorThread class. This interface should not
 * be implemented directly. Instead, use one of the subinterfaces
 * define speciic functionality for a particular event.
 *  
 * @author Nuno Santos 
 */
public interface SelectorHandler {
}
