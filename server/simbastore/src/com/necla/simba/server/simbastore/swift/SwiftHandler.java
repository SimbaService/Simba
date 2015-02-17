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
package com.necla.simba.server.simbastore.swift;

import static com.google.common.io.Closeables.closeQuietly;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.swift.CommonSwiftAsyncClient;
import org.jclouds.openstack.swift.CommonSwiftClient;
import org.jclouds.openstack.swift.blobstore.SwiftBlobStore;
import org.jclouds.openstack.swift.domain.SwiftObject;
import org.jclouds.rest.RestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Bytes;
import com.google.inject.Module;
import com.necla.simba.server.Preferences;
import com.necla.simba.server.simbastore.cache.ChunkCache;
import com.necla.simba.server.simbastore.stats.IOStats;
import com.necla.simba.server.simbastore.swift.SwiftHandler;

public class SwiftHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(SwiftHandler.class);

	private static final String provider = "swift";
	private static String identity; // tenantName:userName
	private static String password;
	private static String container;
	private static String url;
	private static BlobStore storage;
	private static RestContext<CommonSwiftClient, CommonSwiftAsyncClient> swift;
	private ChunkCache chunkCache;

	Random rnd = new Random();
	
	public SwiftHandler(Properties props) {
		url = props.getProperty("swift.proxy.url");
		identity = props.getProperty("swift.identity");
		password = props.getProperty("swift.password");
		container = props.getProperty("swift.container");

		Iterable<Module> modules = ImmutableSet
				.<Module> of(new SLF4JLoggingModule());

		BlobStoreContext context = ContextBuilder.newBuilder(provider)
				.endpoint(url).credentials(identity, password).modules(modules)
				.buildView(BlobStoreContext.class);
		storage = context.getBlobStore();
		swift = context.unwrap();

		boolean enableChunkCache = Boolean.parseBoolean(props.getProperty(
				"chunkcache.enable", "true"));

		if (enableChunkCache) {
			int cacheSizeInMB = Integer.parseInt(props.getProperty(
					"chunkcache.size_mb", "100"));
			if (cacheSizeInMB > 0) {
				chunkCache = new ChunkCache(cacheSizeInMB);
				LOG.debug("Enabling chunk cache size=" + cacheSizeInMB + " MB");
			}
		}

		LOG.info("Started SwiftHandler: url: " + url + ", account: " + identity
				+ ", pass: " + password + ", container: " + container);
	}

	public void putObject(String objectName, byte[] objectBytes) {
		Long start = System.nanoTime();
		LOG.debug("PUT " + objectName);

		SwiftObject object = swift.getApi().newSwiftObject();
		object.getInfo().setName(objectName);
		object.setPayload(objectBytes);
		String result = swift.getApi().putObject(container, object);
	
		LOG.debug("PUT Result: " + result);
		IOStats.putObject(((double) System.nanoTime() - (double) start) / 1000000);
		if (chunkCache != null)
			chunkCache.put(objectName, objectBytes);
	}

	public byte[] getObject(final String objectName) {
		Long start = System.nanoTime();
		byte[] data = null;
		try {
			if (chunkCache == null) {
				data = getUncachedObject(objectName);
			} else {
				data = chunkCache.get(objectName, new Callable<byte[]>() {
					@Override
					public byte[] call() throws Exception {
						
						return getUncachedObject(objectName);
					}
				});
			}
		} catch (IOException e) {
			LOG.error("Could not fetch object; key=" + objectName);
			e.printStackTrace();
		}
		
		IOStats.getObject(((double) System.nanoTime() - (double) start) / 1000000);
		return data;	
	}

	private byte[] getUncachedObject(String objectName) throws IOException {
		LOG.debug("GET " + container + "/" + objectName);
		SwiftObject object = swift.getApi().getObject(container, objectName,
				null);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Payload p = object.getPayload();
		if(p != null){
			p.writeTo(out);
		} else {
			LOG.debug("Object payload is NULL; check swift container status!");
		}
		return out.toByteArray();
	}

	public void deleteObject(String objectName) {
		Long start = System.nanoTime();
		swift.getApi().removeObject(container, objectName);
		IOStats.delObject(((double) System.nanoTime() - (double) start) / 1000000);
		if (chunkCache != null)
			chunkCache.invalidate(objectName);
	}

	@SuppressWarnings("deprecation")
	public void close() {
		closeQuietly(storage.getContext());
	}

	private byte[] readBytes(ByteArrayInputStream bais) {
		byte[] array = new byte[bais.available()];
		try {
			bais.read(array);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return array;
	}
}
