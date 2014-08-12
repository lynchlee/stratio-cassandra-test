package com.stratio.cassandra.lucene.util;

import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.cassandra.lucene.TestingConstants;

public class CassandraUtils {

	private static final Logger logger = Logger.getLogger(CassandraUtils.class);

	private final Cluster cluster;

	private final String host;

	private Metadata metadata;

	private Session session;

	private ConsistencyLevel consistencyLevel;

	public CassandraUtils(String host) {

		String consistencyLevelString = System.getProperty(TestingConstants.CONSISTENCY_LEVEL_CONSTANT_NAME);

		if (consistencyLevelString == null)
			consistencyLevelString = TestingConstants.DEFAULT_CONSISTENCY_LEVEL;

		consistencyLevel = ConsistencyLevel.valueOf(consistencyLevelString);

		this.host = host;
		this.cluster = Cluster.builder().addContactPoint(host).build();
		this.cluster.getConfiguration().getQueryOptions().setConsistencyLevel(consistencyLevel);
		metadata = cluster.getMetadata();
		logger.debug("Connected to cluster (" + this.host + "): " + metadata.getClusterName() + "\n");
		session = cluster.connect();
	}

	public List<Row> execute(String query) {
		waitForIndexRefresh();
		return session.execute(query).all();
	}

	public void execute(String... queriesList) {
		for (String query : queriesList) {
			session.execute(query);
		}
		waitForIndexRefresh();
	}

	public void execute(Collection<String> queriesList) {
		for (String query : queriesList) {
			session.execute(query);
		}
		waitForIndexRefresh();
	}

	public void disconnect() {
		session.close();
	}

	public void waitForIndexRefresh() {

		// Waiting for the custom index to be refreshed
		logger.debug("Waiting for the index to be refreshed...");
		try {
			Thread.sleep(TestingConstants.WAIT_TIME);
		} catch (InterruptedException e) {
			logger.error("Interruption catched during a Thread.sleep; index might be unstable");
		}
		logger.debug("Index ready to rock!");
	}
	
}
