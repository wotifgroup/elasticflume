package org.elasticsearch.flume;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.EventSink;
import org.elasticsearch.cluster.ClusterName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static junit.framework.Assert.*;

public class ElasticSearchSinkBuilderTest {

    @Mock
    Context context;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testThatNoArgConstructorDoesNiceDefaults() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context);

        assertEquals(ClusterName.DEFAULT.value(), esSink.getClusterName());
        assertEquals("flume", esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);


    }

    @Test
    public void testThatSingleArgParameterWorksAsExpected() {

        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, "mycluster");

        assertEquals("mycluster", esSink.getClusterName());
        assertEquals("flume", esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);

    }


    @Test
    public void testThatTwoArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, "mycluster", "myindex");

        assertEquals("mycluster", esSink.getClusterName());
        assertEquals("myindex", esSink.getIndexName());
        assertEquals(0, esSink.getHostNames().length);

    }

    @Test
    public void testThatThreeArgParameterWorksAsExpected() {
        ElasticSearchSink esSink = (ElasticSearchSink) new ElasticSearchSinkBuilder().build(context, "mycluster", "myindex", "host1,host2");

        assertEquals("mycluster", esSink.getClusterName());
        assertEquals("myindex", esSink.getIndexName());
        assertEquals(2, esSink.getHostNames().length);
        assertTrue(Arrays.equals(new String[]{"host1", "host2"}, esSink.getHostNames()));


    }


}
