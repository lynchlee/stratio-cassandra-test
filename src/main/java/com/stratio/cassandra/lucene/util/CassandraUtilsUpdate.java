package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 *
 */
public class CassandraUtilsUpdate {

    private CassandraUtils parent;
    private Update update;

    public CassandraUtilsUpdate(CassandraUtils parent) {
        this.parent = parent;
        update = QueryBuilder.update(parent.getKeyspace(), parent.getTable());
    }

    public CassandraUtilsUpdate set(String name, Object value) {
        update.with(QueryBuilder.set(name, value));
        return this;
    }

    public CassandraUtilsUpdate where(String name, Object value) {
        update.where(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtilsUpdate and(String name, Object value) {
        update.where().and(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtils execute() {
        parent.execute(update);
        return parent;
    }
}
