package com.stratio.cassandra.lucene.querytype;

public enum BooleanSubqueryType {
    MUST("must"), SHOULD("should");

    private final String type;

    BooleanSubqueryType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }
}
