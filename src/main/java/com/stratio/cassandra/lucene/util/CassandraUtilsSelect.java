package com.stratio.cassandra.lucene.util;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.stratio.cassandra.lucene.TestingConstants;
import com.stratio.cassandra.lucene.search.SearchBuilder;
import com.stratio.cassandra.lucene.search.SearchBuilders;
import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder;
import com.stratio.cassandra.lucene.search.sort.builder.SortFieldBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by adelapena on 09/10/14.
 */
public class CassandraUtilsSelect {

    private CassandraUtils parent;
    private SearchBuilder searchBuilder;
    private LinkedList<Clause> clauses;
    private LinkedList<String> extras;
    private Integer limit;
    private Integer fetchSize = TestingConstants.FETCH_SIZE;
    private Boolean refresh = true;

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

    public CassandraUtilsSelect sort(SortFieldBuilder... sort) {
        if (searchBuilder == null) {
            searchBuilder = SearchBuilders.sort(sort);
        } else {
            searchBuilder.sort(sort);
        }
        return this;
    }

    public CassandraUtilsSelect fetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public CassandraUtilsSelect refresh(boolean refresh) {
        this.refresh = refresh;
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
            where.and(QueryBuilder.eq(parent.getIndexColumn(), searchBuilder.refresh(refresh).toJson()));
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
        return parent.execute(sb, fetchSize);
    }

    public Row getFirst() {
        List<Row> rows = get();
        return rows.isEmpty() ? null : rows.get(0);
    }

    public int count() {
        return get().size();
    }

    public Integer[] intColumn(String name) {
        List<Row> rows = get();
        Integer[] values = new Integer[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getInt(name);
        }
        return values;
    }

    public Long[] longColumn(String name) {
        List<Row> rows = get();
        Long[] values = new Long[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getLong(name);
        }
        return values;
    }

    public Float[] floatColumn(String name) {
        List<Row> rows = get();
        Float[] values = new Float[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getFloat(name);
        }
        return values;
    }
    public String[] stringColumn(String name) {
        List<Row> rows = get();
        String[] values = new String[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getString(name);
        }
        return values;
    }
    public Double[] doubleColumn(String name) {
        List<Row> rows = get();
        Double[] values = new Double[rows.size()];
        int count = 0;
        for (Row row : rows) {
            values[count++] = row.getDouble(name);
        }
        return values;
    }
}
