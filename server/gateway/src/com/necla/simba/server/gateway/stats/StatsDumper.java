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
package com.necla.simba.server.gateway.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.gateway.stats.StatsDumper;
import com.necla.simba.server.netio.Stats;



public class StatsDumper extends Thread {
	  private static final Logger LOG = LoggerFactory.getLogger(StatsDumper.class);


	private volatile boolean stopRequested = false;
	public StatsDumper() {
		super("StatsDumper");
		Stats.start();
	}
	
	public void requestStop() {
		stopRequested = true;
	}
	
	@Override
	public void run() {
		while (!stopRequested) {
			
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				break;
			}
		
		}
	}
}
