package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.stratio.cassandra.index.query.builder.ConditionBuilder;
import com.stratio.cassandra.index.query.builder.SearchBuilder;
import com.stratio.cassandra.index.query.builder.SearchBuilders;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by adelapena on 09/10/14.
 */
public class CassandraUtilsSelect {

    private static final Logger logger = Logger.getLogger(CassandraUtilsSelect.class);

    private CassandraUtils parent;
    private SearchBuilder searchBuilder;
    private LinkedList<Clause> clauses;
    private LinkedList<String> extras;
    private Integer limit;

    public CassandraUtilsSelect(CassandraUtils parent) {
        this.parent = parent;
        this.clauses = new LinkedList<>();
        this.extras = new LinkedList<>();
    }

    public CassandraUtilsSelect andEq(String name, Object value) {
        clauses.add(QueryBuilder.eq(name, value));
        return this;
    }

    public CassandraUtilsSelect andGt(String name, Object value) {
        clauses.add(QueryBuilder.gt(name, value));
        return this;
    }

    public CassandraUtilsSelect andGte(String name, Object value) {
        clauses.add(QueryBuilder.gte(name, value));
        return this;
    }

    public CassandraUtilsSelect andLt(String name, Object value) {
        clauses.add(QueryBuilder.lt(name, value));
        return this;
    }

    public CassandraUtilsSelect andLte(String name, Object value) {
        clauses.add(QueryBuilder.lte(name, value));
        return this;
    }

    public CassandraUtilsSelect and(String extra) {
        extras.add(extra);
        return this;
    }

    public CassandraUtilsSelect search() {
        this.searchBuilder = SearchBuilders.search();
        return this;
    }

    public CassandraUtilsSelect query(ConditionBuilder<?, ?> condition) {
        if (searchBuilder == null) {
            searchBuilder = SearchBuilders.query(condition);
        } else {
            searchBuilder.query(condition);
        }
        return this;
    }

    public CassandraUtilsSelect filter(ConditionBuilder<?, ?> condition) {
        if (searchBuilder == null) {
            searchBuilder = SearchBuilders.filter(condition);
        } else {
            searchBuilder.filter(condition);
        }
        return this;
    }

    public CassandraUtilsSelect limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public List<Row> get() {
        Select.Where where = QueryBuilder.select().from(parent.getKeyspace(), parent.getTable()).where();
        for (Clause clause : clauses) {
            where.and(clause);
        }
        if (searchBuilder != null) {
            logger.info("ADDING SEARCH BUILDER FOR " + searchBuilder.toJson());
            where.and(QueryBuilder.eq(parent.getIndexColumn(), searchBuilder.toJson()));
        }
        BuiltStatement statement = limit == null ? where : where.limit(limit);

        String query = statement.toString();
        query = query.substring(0, query.length() - 1);
        StringBuilder sb = new StringBuilder(query);
        for (String extra : extras) {
            sb.append(" ");
            sb.append(extra);
            sb.append(" ");
        }
        return parent.execute(sb);
    }

    public int count() {
        return get().size();
    }

}
