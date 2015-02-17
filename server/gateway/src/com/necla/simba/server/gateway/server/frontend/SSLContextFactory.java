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

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SSLContextFactory {
    private static final SSLContext SERVER_CONTEXT;

	public static SSLContext createSSLContext(
			boolean clientMode, 
			String keystore, 
			String password, String trustStore, String trustPassword) 
	throws Exception {
		// Create/initialize the SSLContext with key material
		char[] passphrase = password.toCharArray();
		// First initialize the key and trust material.
		KeyStore ks = KeyStore.getInstance("JKS");
		ks.load(new FileInputStream(keystore), passphrase);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		
		if (clientMode) {
			// TrustManager's decide whether to allow connections.
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			tmf.init(ks);
			sslContext.init(null, tmf.getTrustManagers(), null);
			
		} else {
			// KeyManager's decide which key material to use.
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, passphrase);
			
			if (trustStore != null) {
				KeyStore ts = KeyStore.getInstance("JKS");
				ts.load(new FileInputStream(trustStore), trustPassword.toCharArray());
				TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
				tmf.init(ts);
				System.out.println("Using the trust store for client auth");
				sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
			} else
				sslContext.init(kmf.getKeyManagers(), null, null);
		}
		return sslContext;
	}
    static {
    	try {
    	    SERVER_CONTEXT = createSSLContext(false, "server.jks", "mobeeus123", "server_truststore.jks", "mobeeus123");
    	} catch (Exception e) {
    		System.err.println("Could not initialize SSL context");
    		throw new Error(e);
    	}
    }

    public static SSLContext getServerContext() {
        return SERVER_CONTEXT;
    }

   

    private SSLContextFactory() {
        // Unused
    }

}
