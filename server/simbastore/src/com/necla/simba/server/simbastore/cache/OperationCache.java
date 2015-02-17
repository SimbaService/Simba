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
package com.necla.simba.server.simbastore.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.necla.simba.server.simbastore.cache.ChangeSet;
import com.necla.simba.server.simbastore.cache.ObjectChanges;
import com.necla.simba.server.simbastore.cache.OperationCache;
import com.necla.simba.server.simbastore.cache.OperationCache.CircularQueue;

/**
 * @author dperkins@nec-labs.com
 * @created May 31, 2013 10:36:58 AM
 */

public class OperationCache {
	private static final Logger LOG = LoggerFactory
			.getLogger(OperationCache.class);

	CircularQueue<Integer> cq;
	private TreeMap<Integer, String> cache;
	private HashMap<String, TreeMap<Integer, ChangeSet>> changeLog;
	private int MAX_SIZE;
	private boolean ENABLE_CACHE;

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	public OperationCache(Properties props) {
		// Initialize operation in-memory cache
		MAX_SIZE = Integer.parseInt(props.getProperty("cache.size.limit"));
		ENABLE_CACHE = Boolean.parseBoolean(props.getProperty("cache.enable"));
		cache = new TreeMap<Integer, String>();
		cq = new CircularQueue<Integer>(MAX_SIZE);

		/*
		 * change log:
		 * 
		 * key = row_id string
		 * 
		 * value = treemap (key=version, value = changeset)
		 * 
		 * this allows us to query all changes between N versions for a given
		 * row.
		 */

		changeLog = new HashMap<String, TreeMap<Integer, ChangeSet>>();
	}

	public boolean containsRange(Integer lowVersion, Integer highVersion) {
		read.lock();
		try {
			if (!cache.isEmpty()) {
				if (cache.firstKey() <= lowVersion
						&& cache.lastKey() >= highVersion) {
					return true;
				} else {
					return false;
				}
			}
			return false;
		} finally {
			read.unlock();
		}
	}

	public void put(Integer version, String row) {
		if (ENABLE_CACHE) {

			write.lock();
			try {
				// if size limit is reached, remove lowest key before insert
				if (cache.size() >= MAX_SIZE) {
					cache.remove(cache.firstKey());
				}

				// Add to cache
				cache.put(version, row);
				LOG.info("OperationCache: Inserted '" + row + "' version "
						+ version);

				// Add version to circular queue
				// This will cause eviction of the LRU entry
				cq.add(version);
			} finally {
				write.unlock();
			}
		}
	}

	public SortedMap<Integer, String> getRange(Integer lowVersion,
			Integer highVersion) {
		read.lock();
		try {
			return cache.subMap(lowVersion, true, highVersion, true);
		} finally {
			read.unlock();
		}
	}

	public ChangeSet getChanges(String key, Integer firstVersion,
			Integer lastVersion) {

		read.lock();
		try {
			if (changeLog.containsKey(key)) {
				// get all changes for this rowkey
				TreeMap<Integer, ChangeSet> row_changes = changeLog.get(key);

				LOG.debug("Getting changeset for row=" + key + ", versions=["
						+ firstVersion + "," + lastVersion + "]");
				LOG.debug(row_changes.keySet().toString());

				SortedMap<Integer, ChangeSet> changes = null;
				// get the slice of changes for the requested range of versions
				try {
					changes = row_changes.subMap(firstVersion, true,
							lastVersion, true);
					LOG.debug("changeset size=" + changes.size());
				} catch (IllegalArgumentException e) {
					LOG.error("Error: Change Log getChanges(): "
							+ e.getMessage());
					return null;
				}

				Set<String> table_changes = new HashSet<String>();
				ChangeSet all_changes = new ChangeSet();
				all_changes.version = 0;

				Map<String, Integer> origSize = new HashMap<String, Integer>();

				// Flatten the ChangeSets into one aggregate ChangeSet.
				// The map is sorted so all versions should be iterated in
				// order.
				Iterator<Entry<Integer, ChangeSet>> it = changes.entrySet()
						.iterator();

				// for every change set
				while (it.hasNext()) {
					Entry<Integer, ChangeSet> e = it.next();
					ChangeSet cs = e.getValue();

					// drop all changes if a delete is found
					if (cs.deleted == true) {
						table_changes.clear();
						all_changes.object_changes.clear();
					}

					// add all table changes to list
					table_changes.addAll(cs.table_changes);

					// merge all chunk changes by index (offset)
					for (ObjectChanges oc : cs.object_changes) {

						// NOTE: ObjectChanges equals comparator uses only the
						// 'name' of the object column
						if (all_changes.object_changes.contains(oc)) {

							// this object already exists in the aggregated
							// change object set, retrieve it
							ObjectChanges x = all_changes.object_changes
									.get(all_changes.object_changes.indexOf(oc));

							// add or replace all changed chunks
							Iterator<Entry<Integer, UUID>> it2 = oc.chunks
									.entrySet().iterator();
							while (it2.hasNext()) {
								Entry<Integer, UUID> e2 = it2.next();

								// insert the new chunk
								x.chunks.put(e2.getKey(), e2.getValue());
							}

							// set the size to the new size
							x.size = oc.size;

						} else {
							// save the original size of this object
							origSize.put(oc.name, oc.size);

							// this object has not yet been added to the
							// aggregated change object set, add this copy
							all_changes.object_changes.add(oc);

						}
					}

					/*
					 * determine for all changed objects if they have been
					 * truncated by comparing the size on first change in the
					 * set to the size at the last change in this set
					 */
					for (ObjectChanges oc : all_changes.object_changes) {

						// drop all chunks with indexes higher than current
						// number of chunks (size)
						Iterator<Integer> it2 = oc.chunks.keySet().iterator();
						while (it2.hasNext()) {
							int idx = it2.next();

							LOG.debug("idx=" + idx);
							LOG.debug("oc.size=" + oc.size);

							if (idx >= oc.size) {
								it2.remove();
							}
						}

						// if the objects number of chunks (size) has not
						// reduced, set it to null
						// if (oc.size >= origSize.size()) {
						// oc.size = null;
						// }

					}

					LOG.debug("\nobjects=" + cs.object_changes.toString());

					// update row version
					all_changes.version = cs.version;
				}

				all_changes.table_changes.addAll(table_changes);
				// all_changes.object_changes.addAll(object_changes);

				LOG.debug("\nAGGREGATED CHANGES:\n\n " + all_changes.toString()
						+ "\n");

				return all_changes;
			} else {
				return null;
			}
		} finally {
			read.unlock();
		}
	}

	public void putChanges(String rowkey, Integer version, ChangeSet change) {
		if (ENABLE_CACHE) {
			write.lock();
			try {
				TreeMap<Integer, ChangeSet> row_changes = null;
				if (changeLog.containsKey(rowkey)) {
					row_changes = changeLog.get(rowkey);
				} else {
					row_changes = new TreeMap<Integer, ChangeSet>();
					changeLog.put(rowkey, row_changes);
				}
				row_changes.put(version, change);
			} finally {
				write.unlock();
			}
		}
	}

	public class CircularQueue<E> extends LinkedList<E> {
		private static final long serialVersionUID = -2156693473431551308L;
		private int limit;

		public CircularQueue(int limit) {
			this.limit = limit;
		}

		@Override
		public boolean add(E o) {
			super.add(o);
			while (size() > limit) {
				// remove and return the LRU version
				E o2 = super.remove();

				// get the rowkey corresponding to this version
				if (cache.containsKey(o2)) {
					String rowkey = cache.get(o2);

					// remove the version from that rowkey's treemap
					if (changeLog != null && changeLog.containsKey(rowkey)) {
						changeLog.get(rowkey).remove(o2);
					}
					// remove the version from the cache
					cache.remove(o2);
				}
			}
			return true;
		}
	}

}
