package com.stratio.cassandra.lucene;

public final class TestingConstants {

    public static final String CASSANDRA_LOCALHOST_CONSTANT = "127.0.0.1";

    public static final String KEYSPACE_CONSTANT = "lucene";

    public static final int REPLICATION_FACTOR_2_CONSTANT = 2;

    public static final String TABLE_NAME_CONSTANT = "table_index_test";

    public static final String INDEX_NAME_CONSTANT = "lucene_index";

    public static final String INDEX_COLUMN_CONSTANT = "lucene";

    // Query types optional parameters
    public static final String MAX_EDITS_PARAM_CONSTANT = "max_edits";

    public static final String PREFIX_LENGTH_PARAM_CONSTANT = "prefix_length";

    public static final String MAX_EXPANSIONS_PARAM_CONSTANT = "max_expansions";

    public static final String TRANSPOSITIONS_PARAM_CONSTANT = "transpositions";

    public static final String SLOP_PARAM_CONSTANT = "slop";

    public static final String LOWER_PARAM_CONSTANT = "lower";

    public static final String INCLUDE_LOWER_PARAM_CONSTANT = "include_lower";

    public static final String UPPER_PARAM_CONSTANT = "upper";

    public static final String INCLUDE_UPPER_PARAM_CONSTANT = "include_upper";

    public static final String BOOST_PARAM_CONSTANT = "boost";

    // Boolean query type conditions
    public static final String MUST_PARAM_CONSTANT = "must";

    public static final String SHOULD_PARAM_CONSTANT = "should";

    public static final String NOT_PARAM_CONSTANT = "not";
}
