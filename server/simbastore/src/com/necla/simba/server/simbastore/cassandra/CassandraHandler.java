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
package com.necla.simba.server.simbastore.cassandra;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryTimeoutException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.necla.simba.protocol.Common.Column;
import com.necla.simba.protocol.Common.ColumnData;
import com.necla.simba.protocol.Common.SimbaConsistency;
import com.necla.simba.server.simbastore.stats.IOStats;

public class CassandraHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(CassandraHandler.class);

	private static String seed;
	private static String keyspaceName;
	private Cluster cluster;
	private Session session;
	private Properties properties;

	public static final String VERSION = "version";
	public static final String DELETED = "deleted";
	public static final String KEY = "key";

	private static final Set<String> meta;

	static {
		meta = new HashSet<String>();
		String[] fields = { VERSION, DELETED, KEY };
		for (String f : fields)
			meta.add(f);
	}

	// Cassandra accepts Integer timestamps.
	// We can use a local timestamp (technically, a counter) since any
	// SimbaTable table belongs to a single SimbaStore node.
	private Long counter = System.currentTimeMillis();

	private static PrintWriter out;

	static String[] excluded = { "system", "system_auth", "system_traces" };
	static List<String> exclusions = Arrays.asList(excluded);

	private Column.Type toSimbaType(String casType) {
		if (casType.equals("text"))
			return Column.Type.VARCHAR;
		else if (casType.equals("int"))
			return Column.Type.INT;
		else if (casType.equals("list<uuid>"))
			return Column.Type.OBJECT;
		else if (casType.equals("uuid"))
			return Column.Type.UUID;
		else if (casType.equals("boolean"))
			return Column.Type.BOOLEAN;
		else if (casType.equals("bigint"))
			return Column.Type.BIGINT;
		else if (casType.equals("blob"))
			return Column.Type.BLOB;
		else if (casType.equals("double"))
			return Column.Type.DOUBLE;
		else if (casType.equals("float"))
			return Column.Type.FLOAT;
		else if (casType.equals("counter"))
			return Column.Type.COUNTER;
		else if (casType.equals("timestamp"))
			return Column.Type.TIMESTAMP;
		else if (casType.equals("varint"))
			return Column.Type.VARINT;
		else if (casType.equals("inet"))
			return Column.Type.INET;

		throw new RuntimeException("Unsupported type " + casType);
	}

	private String toCasType(Column.Type simbaType) {
		switch (simbaType.getNumber()) {
		case Column.Type.VARCHAR_VALUE:
			return "text";
		case Column.Type.INT_VALUE:
			return "int";
		case Column.Type.OBJECT_VALUE:
			return "list<uuid>";
		case Column.Type.UUID_VALUE:
			return "uuid";
		case Column.Type.BIGINT_VALUE:
			return "bigint";
		case Column.Type.BOOLEAN_VALUE:
			return "boolean";
		case Column.Type.BLOB_VALUE:
			return "blob";
		case Column.Type.DOUBLE_VALUE:
			return "double";
		case Column.Type.FLOAT_VALUE:
			return "float";
		case Column.Type.COUNTER_VALUE:
			return "counter";
		case Column.Type.TIMESTAMP_VALUE:
			return "timestamp";
		case Column.Type.VARINT_VALUE:
			return "varint";
		case Column.Type.INET_VALUE:
			return "inet";
		default:
			throw new RuntimeException("Unsupported type " + simbaType);
		}
	}

	public CassandraHandler(Properties props) {
		this.properties = props;
		seed = props.getProperty("cassandra.seed");
		keyspaceName = props.getProperty("cassandra.keyspace");
		LOG.info("Started CassandraHandler: seed: " + seed
				+ " default keyspace: " + keyspaceName);

		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(
					"cassandra.log")));
		} catch (IOException e) {
			e.printStackTrace();
		}
		// flush the print buffer every N seconds (N = 30)
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				// LOG.debug("Flushing print buffer...");
				out.flush();
			}
		}, 0, 10000);

		connect();
	}

	public void connect() {

		cluster = new Cluster.Builder().addContactPoint(seed).build();

		// final int core_threads = 1;
		// final int max_threads = 4;
		//
		// PoolingOptions p = cluster.getConfiguration().getPoolingOptions();
		// p.setMaxConnectionsPerHost(HostDistance.LOCAL, max_threads);
		// p.setCoreConnectionsPerHost(HostDistance.LOCAL, core_threads);

		// SocketOptions so = cluster.getConfiguration().getSocketOptions();
		// so.setTcpNoDelay(true).setReuseAddress(true).setKeepAlive(true);

		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());

		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}

		session = cluster.connect();

		// restoreTables(cluster);
	}

	private ResultSet executeQuery(String statement) {
		ResultSet result = null;

		try {
			result = session.execute(statement);
		} catch (AlreadyExistsException e) {
			System.out.println("Keyspace or table already exists");
		} catch (NoHostAvailableException e) {
			System.out
					.printf("No host in the %s cluster can be contacted to execute the query.\n",
							session.getCluster());
			e.printStackTrace();
		} catch (QueryTimeoutException e) {
			System.out
					.println("An exception has been thrown by Cassandra because the query execution has timed out.");
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			System.out
					.println("An exception was thrown by Cassandra because it cannot "
							+ "successfully execute the query with the specified consistency level.");
			e.printStackTrace();
		} catch (SyntaxError e) {
			System.out.printf(
					"The query '%s' has a syntax error.\n message=%s",
					statement, e.getMessage());
			e.printStackTrace();
		} catch (QueryValidationException e) {
			System.out
					.printf("The query '%s' is not valid, for example, incorrect syntax.\n",
							statement);
			e.printStackTrace();
		} catch (IllegalStateException e) {
			System.out.println("The BoundStatement is not ready.");
			e.printStackTrace();
		}

		return result;
	}

	private ResultSet executeQuery(Query query) {
		ResultSet result = null;

		try {
			result = session.execute(query);
		} catch (AlreadyExistsException e) {
			System.out.println("Keyspace or table already exists");
		} catch (NoHostAvailableException e) {
			System.out
					.printf("No host in the %s cluster can be contacted to execute the query.\n",
							session.getCluster());
		} catch (QueryExecutionException e) {
			System.out
					.println("An exception was thrown by Cassandra because it cannot "
							+ "successfully execute the query with the specified consistency level.");
		} catch (QueryValidationException e) {
			System.out
					.printf("The query \n%s \nis not valid, for example, incorrect syntax.\n",
							query.toString());
		} catch (IllegalStateException e) {
			System.out.println("The BoundStatement is not ready.");
		}

		return result;
	}

	public int getVersion(String keyspace, String table, ConsistencyLevel level) {
		int version = -1;

		// Build SELECT query
		Query query = QueryBuilder.select(VERSION)
				.from((keyspace == null) ? keyspaceName : keyspace, table)
				.setConsistencyLevel(level);

		ResultSet result = executeQuery(query);

		if (result.isExhausted()) {
			return -1;
		}

		for (Row row : result) {
			int tmp = row.getInt(VERSION);
			if (version < tmp) {
				version = tmp;
			}
		}

		return version;
	}

	public void createKeyspace(String keyspaceName, Integer replication) {
		try {
			session.execute("CREATE KEYSPACE " + keyspaceName
					+ " WITH replication " + "= {'class':'SimpleStrategy', "
					+ "'replication_factor':" + replication.toString() + "};");
		} catch (AlreadyExistsException e) {
			System.out.println("Keyspace " + keyspaceName
					+ " already exists -- ignoring!");
		}
	}

	public List<Column> getSchema(String keySpace, String tableName) {
		Metadata m = session.getCluster().getMetadata();
		KeyspaceMetadata km = m.getKeyspace(keySpace);
		if (km == null)
			return null;
		TableMetadata tm = km.getTable(tableName);
		if (tm == null)
			return null;
		// build schema
		List<Column> columns = new LinkedList<Column>();
		for (ColumnMetadata cm : tm.getColumns()) {
			if (!meta.contains(cm.getName()))
				columns.add(Column.newBuilder().setName(cm.getName())
						.setType(toSimbaType(cm.getType().toString())).build());
		}

		return columns;

	}

	public void createTable(String keyspace, String tableName, List<Column> list) {

		// Create table
		StringBuilder command = new StringBuilder();
		command.append("CREATE TABLE ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" (").append(KEY)
				.append(" text PRIMARY KEY, ").append(VERSION).append(" int, ")
				.append(DELETED).append(" boolean, ");

		Column pair = list.get(0);
		command.append(pair.getName()).append(" ")
				.append(toCasType(pair.getType()));

		for (int i = 1; i < list.size(); i++) {
			pair = list.get(i);
			command.append(", ").append(pair.getName()).append(" ")
					.append(toCasType(pair.getType()));
		}
		command.append(");");

		executeQuery(command.toString());

		// add some delay (in seconds) to hopefully avoid issue where index is
		// not created successfully.
		// int delay = 5;
		// try {
		// Thread.sleep(delay * 1000);
		// } catch (InterruptedException e){
		// e.printStackTrace();
		// }

		// Create secondary index on version
		command = new StringBuilder();
		command.append("CREATE INDEX ON ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" (").append(VERSION)
				.append(");");
		LOG.debug(command.toString());

		executeQuery(command.toString());
	}

	public void putRow(String keyspace, String tableName, String rowKey,
			Integer version, List<ColumnData> values, ConsistencyLevel level) {
		Long start = System.nanoTime();
		LOG.debug("PUT ROW START\n");

		StringBuilder command = new StringBuilder();
		StringBuilder vals = new StringBuilder();

		command.append("INSERT INTO ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" (").append(KEY)
				.append(",").append(VERSION).append(",").append(DELETED)
				.append(",");

		ColumnData pair = values.get(0);
		command.append(pair.getColumn());
		vals.append(pair.getValue());

		for (int i = 1; i < values.size(); i++) {
			pair = values.get(i);
			command.append(",").append(pair.getColumn());
			vals.append(",").append(pair.getValue());
		}

		command.append(") VALUES (").append("'" + rowKey + "',")
				.append(version + ",").append("false,").append(vals.toString())
				.append(") USING TIMESTAMP ").append(++this.counter)
				.append(";");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		LOG.debug(ss.toString());
		executeQuery(ss);
		LOG.debug("PUT ROW END\n\n");
		IOStats.putRow(((double) System.nanoTime() - (double) start) / 1000000);

	}

	public void putSubscription(String keyspace, String tableName,
			String rowKey, ByteBuffer subscription, ConsistencyLevel level) {

		Query query = QueryBuilder.update(keyspace, tableName)
				.with(QueryBuilder.append("subscriptions", subscription))
				.where(QueryBuilder.eq(KEY, UUID.fromString(rowKey)))
				.setConsistencyLevel(level);

		session.execute(query);
	}

	public void setTableConsistencyLevel(String rowKey,
			SimbaConsistency.Type consistencyLevel) {
		Query query = QueryBuilder.insertInto("simbastore", "metadata")
				.value(KEY, rowKey).value("consistency", consistencyLevel)
				.setConsistencyLevel(ConsistencyLevel.ALL);
		session.execute(query);
	}

	public void putRowWithTracing(String keyspace, String tableName,
			String rowKey, Integer version, List<ColumnData> values,
			ConsistencyLevel level) {

		StringBuilder command = new StringBuilder();
		StringBuilder vals = new StringBuilder();

		command.append("INSERT INTO ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" (").append(KEY)
				.append(",").append(VERSION).append(",").append(DELETED)
				.append(",");

		ColumnData pair = values.get(0);
		command.append(pair.getColumn());
		vals.append(pair.getValue());

		for (int i = 1; i < values.size(); i++) {
			pair = values.get(i);
			command.append(",").append(pair.getColumn());
			vals.append(",").append(pair.getValue());
		}

		command.append(") VALUES (").append("'" + rowKey + "',")
				.append(version + ",").append("false,").append(vals.toString())
				.append(");");
		LOG.debug(command.toString());

		SimpleStatement ss = new SimpleStatement(command.toString());
		Query insert = QueryBuilder.batch(ss).setConsistencyLevel(level)
				.enableTracing();

		ResultSet results = session.execute(insert);
		ExecutionInfo executionInfo = results.getExecutionInfo();
		System.out.printf("Host (queried): %s\n", executionInfo
				.getQueriedHost().toString());
		for (Host host : executionInfo.getTriedHosts()) {
			System.out.printf("Host (tried): %s\n", host.toString());
		}
		QueryTrace queryTrace = executionInfo.getQueryTrace();
		System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
		System.out.printf("%-38s | %-12s | %-10s | %-12s\n", "activity",
				"timestamp", "source", "source_elapsed");
		System.out
				.println("---------------------------------------+--------------+------------+--------------");
		for (QueryTrace.Event event : queryTrace.getEvents()) {
			System.out.printf("%38s | %12s | %10s | %12s\n",
					event.getDescription(), new Date(event.getTimestamp()),
					event.getSource(), event.getSourceElapsedMicros());
		}
		insert.disableTracing();

	}

	public ResultSet getRowWithTracing(String keyspace, String table,
			String key, ConsistencyLevel level) {

		Query select = QueryBuilder.select().all().from(keyspace, table)
				.where(QueryBuilder.eq(KEY, key)).setConsistencyLevel(level)
				.enableTracing();

		ResultSet results = session.execute(select);
		ExecutionInfo executionInfo = results.getExecutionInfo();
		System.out.printf("Host (queried): %s\n", executionInfo
				.getQueriedHost().toString());
		for (Host host : executionInfo.getTriedHosts()) {
			System.out.printf("Host (tried): %s\n", host.toString());
		}
		QueryTrace queryTrace = executionInfo.getQueryTrace();
		System.out.printf("Trace id: %s\n\n", queryTrace.getTraceId());
		System.out.printf("%-38s | %-12s | %-10s | %-12s\n", "activity",
				"timestamp", "source", "source_elapsed");
		System.out
				.println("---------------------------------------+--------------+------------+--------------");
		for (QueryTrace.Event event : queryTrace.getEvents()) {
			System.out.printf("%38s | %12s | %10s | %12s\n",
					event.getDescription(), new Date(event.getTimestamp()),
					event.getSource(), event.getSourceElapsedMicros());
		}
		select.disableTracing();
		return results;
	}

	public ResultSet getRow(String keyspace, String table, String key,
			ConsistencyLevel level) {
		Long start = System.nanoTime();
		// LOG.debug("GET ROW START\n");

		Query query = QueryBuilder.select().all().from(keyspace, table)
				.where(QueryBuilder.eq(KEY, key)).setConsistencyLevel(level);

		LOG.debug(query.toString());
		ResultSet result = executeQuery(query);

		IOStats.getRow(((double) System.nanoTime() - (double) start) / 1000000);

		return result;
	}

	public ResultSet getRows(String keyspace, String table, int limit,
			String startKey, ConsistencyLevel level) {
		StringBuilder command = new StringBuilder();
		command.append("SELECT * from ").append(keyspace).append(".")
				.append(table);
		if (startKey != null) {
			command.append(" WHERE token(key)>token('").append(startKey)
					.append("')");
		}

		if (limit > 0) {
			command.append(" LIMIT ").append(Integer.toString(limit));
		}
		command.append(";");
		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);
		ResultSet result = executeQuery(ss);
		return result;
	}

	public ResultSet getSubscriptions(String deviceId, ConsistencyLevel level) {
		LOG.debug("GET SUBSCRIPTIONS START\n");

		StringBuilder command = new StringBuilder();

		command.append("SELECT * FROM simbastore.subscriptions WHERE ")
				.append(KEY).append(" = ").append(deviceId).append(";");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		ResultSet result = executeQuery(ss);

		LOG.debug("GET SUBSCRIPTIONS END\n\n");

		return result;
	}

	public ResultSet getTableConsistencyLevel(String rowKey) {
		Query query = QueryBuilder.select().all()
				.from("simbastore", "metadata")
				.where(QueryBuilder.eq(KEY, rowKey))
				.setConsistencyLevel(ConsistencyLevel.ONE);

		ResultSet result = executeQuery(query);
		return result;
	}

	public ResultSet getColumnFromRow(String keyspace, String tableName,
			String key, String column, ConsistencyLevel level) {

		StringBuilder command = new StringBuilder();

		// Build SELECT query
		command.append("SELECT ").append(column).append(" FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ").append(KEY)
				.append(" = '").append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		ResultSet result = executeQuery(ss);

		return result;
	}

	public ResultSet getColumnsFromRow(String keyspace, String tableName,
			String key, ArrayList<String> columns, ConsistencyLevel level) {

		StringBuilder command = new StringBuilder();
		StringBuilder cols = new StringBuilder();

		// Build column list
		cols.append(columns.get(0));
		for (int i = 1; i < columns.size(); i++) {
			cols.append(",").append(columns.get(i));
		}

		// Build SELECT query
		command.append("SELECT ").append(cols.toString()).append(" FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ").append(KEY)
				.append(" = '").append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		ResultSet result = executeQuery(ss);

		return result;
	}

	public ResultSet getRowByVersion(String keyspace, String tableName,
			int version, ConsistencyLevel level) {
		Long start = System.nanoTime();
		StringBuilder command = new StringBuilder();

		command.append("SELECT * FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ")
				.append(VERSION).append(" = ").append(version).append(";");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		ResultSet result = executeQuery(ss);
		IOStats.getRowByVersion(((double) System.nanoTime() - (double) start) / 1000000);

		return result;
	}

	public void markDeleted(String keyspace, String tableName, String key,
			int version, ConsistencyLevel level) {

		Long start = System.nanoTime();

		StringBuilder command = new StringBuilder();

		command.append("UPDATE ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" USING TIMESTAMP ")
				.append(++this.counter).append(" SET ").append(DELETED)
				.append(" = true,").append(VERSION).append(" = ")
				.append(version).append(" WHERE ").append(KEY).append(" = '")
				.append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		executeQuery(ss);
		IOStats.markDelRow(((double) System.nanoTime() - (double) start) / 1000000);
	}

	public void deleteColumnFromRow(String keyspace, String tableName,
			String key, String column, ConsistencyLevel level) {
		StringBuilder command = new StringBuilder();

		command.append("DELETE ").append(column).append(" FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ").append(KEY)
				.append(" = '").append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		executeQuery(ss);
	}

	public void deleteColumnsFromRow(String keyspace, String tableName,
			String key, List<String> columns, ConsistencyLevel level) {
		StringBuilder command = new StringBuilder();

		command.append("DELETE ");

		String col = columns.get(0);
		command.append(col);

		for (int i = 1; i < columns.size(); i++) {
			col = columns.get(i);
			command.append(",").append(col);
		}

		command.append(" FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ").append(KEY)
				.append(" = '").append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		executeQuery(ss);
	}

	public void deleteRow(String keyspace, String tableName, String key,
			ConsistencyLevel level) {
		Long start = System.nanoTime();
		StringBuilder command = new StringBuilder();

		command.append("DELETE FROM ")
				.append((keyspace == null) ? keyspaceName : keyspace)
				.append(".").append(tableName).append(" WHERE ").append(KEY)
				.append(" = '").append(key).append("';");

		SimpleStatement ss = new SimpleStatement(command.toString());
		ss.setConsistencyLevel(level);

		executeQuery(ss);
		IOStats.delRow(((double) System.nanoTime() - (double) start) / 1000000);

	}

	public void dropTable(String id) {
		executeQuery("DROP TABLE " + id + ";");
	}

	public void shutdown() {
		cluster.shutdown();
	}

	public static void getRowsTest(String keyspace, String table,
			int start_row, int end_row) {
		Properties properties = new Properties();
		try {
			properties.load(CassandraHandler.class
					.getResourceAsStream("/simbastore.properties"));
		} catch (IOException e) {
			System.err.println("Could not load properties: " + e.getMessage());
			System.exit(1);
		}

		CassandraHandler ch = new CassandraHandler(properties);
		LOG.info("connect");

		for (int i = start_row; i <= end_row; i++) {
			long start = System.nanoTime();
			ResultSet resultSet = ch.getRow(keyspace, table, "row" + i,
					ConsistencyLevel.ONE);
			System.out.println(resultSet.one().getString(KEY));
			Double elapsed = ((double) System.nanoTime() - (double) start) / 1000000;
			out.println("GET row" + i + " " + elapsed.toString());
		}

		ch.shutdown();
		LOG.info("shutdown");

		out.flush();

		System.exit(0);
	}

	public static void putRowsTest(String keyspace, String table,
			int start_row, int end_row) {
		Properties properties = new Properties();
		try {
			properties.load(CassandraHandler.class
					.getResourceAsStream("/simbastore.properties"));
		} catch (IOException e) {
			System.err.println("Could not load properties: " + e.getMessage());
			System.exit(1);
		}

		CassandraHandler ch = new CassandraHandler(properties);
		LOG.info("connect");

		List<Column> columns = new LinkedList<Column>();
		for (int i = 0; i < 10; i++) {
			columns.add(Column.newBuilder().setName("col" + i)
					.setType(Column.Type.VARCHAR).build());
		}

		ch.createTable(keyspace, table, columns);

		for (int i = start_row; i <= end_row; i++) {
			List<ColumnData> values = new LinkedList<ColumnData>();
			for (int j = 0; j < 10; ++j) {
				values.add(ColumnData.newBuilder().setColumn("col" + j)
						.setValue("'" + randomString(50) + "'").build());
			}
			ch.putRow(keyspace, table, "row" + i, i, values,
					ConsistencyLevel.ALL);
		}

		ch.shutdown();
		LOG.info("shutdown");

		System.exit(0);
	}

	static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
	static Random rnd = new Random();

	public static String randomString(int len) {

		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	/*
	 * testing
	 */
	public static void main(String[] args) {
		if (args[0].equals("get")) {
			getRowsTest(args[1], args[2], Integer.parseInt(args[3]),
					Integer.parseInt(args[4]));
		} else if (args[0].equals("put")) {
			putRowsTest(args[1], args[2], Integer.parseInt(args[3]),
					Integer.parseInt(args[4]));
		}
	}
}
