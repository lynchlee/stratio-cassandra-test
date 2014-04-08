package com.stratio.cassandra.lucene.util;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraUtils {

    private static final Logger logger = Logger.getLogger(CassandraUtils.class);

    private final Cluster cluster;

    private final String host;

    private Metadata metadata;

    private Session session;

    public CassandraUtils(String host) {
        this.host = host;
        this.cluster = Cluster.builder().addContactPoint(host).build();
        this.cluster.getConfiguration().getQueryOptions()
                .setConsistencyLevel(ConsistencyLevel.QUORUM);
        metadata = cluster.getMetadata();
        logger.debug("Connected to cluster (" + host + "): "
                + metadata.getClusterName() + "\n");
        session = cluster.connect();
    }

    public ResultSet executeQuery(String query) {

        return session.execute(query);
    }

    public void executeQueriesList(List<String> queriesList) {

        for (String query : queriesList) {
            session.execute(query);
        }
    }

    private void connect() {
        // Cluster cluster = Cluster.builder().addContactPoint(host).build();
        // cluster.getConfiguration().getQueryOptions()
        // .setConsistencyLevel(ConsistencyLevel.QUORUM);
        metadata = cluster.getMetadata();
        logger.debug("Connected to cluster (" + host + "): "
                + metadata.getClusterName() + "\n");
        session = cluster.connect();
    }

    public void disconnect() {
        session.close();
    }
}
