package org.elasticsearch.flume;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.util.Arrays;

import com.cloudera.flume.conf.Context;
import org.elasticsearch.cluster.ClusterName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ElasticSearchSinkBuilderTest {

    private static final String CLUSTER_NAME = "mycluster";
    private static final String INDEX_NAME = "myindex";
    private static final String HOST1 = "host1";
    private static final String HOST2 = "host2";
    private static final String HOST_LIST = HOST1 + "," + HOST2;
    private static final String INDEX_TYPE = "myindextype";

    @Mock
    Context context;
    private static final int EXPECTED_HOST_COUNT = 2;
    private static final String DEFAULT_INDEX_TYPE = "log";
    private static final String DEFAULT_INDEX = "flume";

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testThatNoArgConstructorDoesNiceDefaults() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context);
        assertEquals(ClusterName.DEFAULT.value(), esSink.getClusterName());
        assertEquals(DEFAULT_INDEX, esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);
    }

    @Test
    public void testThatSingleArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, CLUSTER_NAME);
        assertEquals(CLUSTER_NAME, esSink.getClusterName());
        assertEquals(DEFAULT_INDEX, esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);
    }

    @Test
    public void testThatTwoArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, CLUSTER_NAME, INDEX_NAME);
        assertEquals(CLUSTER_NAME, esSink.getClusterName());
        assertEquals(INDEX_NAME, esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);
    }

    @Test
    public void testThatThreeArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, CLUSTER_NAME, INDEX_NAME, HOST_LIST);
        assertEquals(CLUSTER_NAME, esSink.getClusterName());
        assertEquals(INDEX_NAME, esSink.getIndexName());
        assertEquals(DEFAULT_INDEX_TYPE, esSink.getIndexType());
        assertEquals(EXPECTED_HOST_COUNT, esSink.getHostNames().length);
        assertTrue(Arrays.equals(new String[]{HOST1, HOST2}, esSink.getHostNames()));
    }

    @Test
    public void testThatFourArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, CLUSTER_NAME, INDEX_NAME, HOST_LIST, INDEX_TYPE);
        assertEquals(CLUSTER_NAME, esSink.getClusterName());
        assertEquals(INDEX_NAME, esSink.getIndexName());
        assertEquals(EXPECTED_HOST_COUNT, esSink.getHostNames().length);
        assertTrue(Arrays.equals(new String[]{HOST1, HOST2}, esSink.getHostNames()));
        assertEquals(INDEX_TYPE, esSink.getIndexType());
    }
}
