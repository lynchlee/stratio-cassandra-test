package com.stratio.cassandra.lucene.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.stratio.cassandra.lucene.breakdown.BreakdownTest;

@RunWith(Suite.class)
@SuiteClasses({ BreakdownTest.class })
public class BreakDownSuite {

}
