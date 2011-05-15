package org.elasticsearch.flume;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.reporter.ReportEvent;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchSinkTest {

    private Node searchNode;

    private Client searchClient;

    @Before
    public void startSearchNode() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local")
                .put("node.local", "true")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "1")
                .build();

        searchNode = nodeBuilder()
                .settings(settings)
                .node();

        searchClient = searchNode.client();

        searchClient.admin()
                .cluster()
                .prepareHealth()
                .setWaitForGreenStatus()
                .execute()
                .actionGet();
    }

    @After
    public void stopSearchNode() throws Exception {
        // Reset the index
        ((InternalNode) searchNode).injector().getInstance(Gateway.class).reset();

        searchClient.close();
        searchNode.stop();
    }

    @Test
    public void appendDifferentTypesOfLogMessage() throws IOException, InterruptedException {
        ElasticSearchSink sink = createAndOpenSink();
        Map<String, byte[]> attributes = new HashMap<String, byte[]>();
        attributes.put("attr1", new String("qux quux quuux").getBytes());
        attributes.put("attr2", new String("value2").getBytes());

        Event event = new EventImpl("message goes here".getBytes(), 0, Priority.INFO, System.nanoTime(),
                "localhost", attributes);

        sink.append(event);
        sink.append(new EventImpl("bleh foo baz bar".getBytes(), 1, Priority.WARN, System.nanoTime(), "notlocalhost"));
        EventImpl jsonEvent = new EventImpl(("{\"host\":\"host.name\",\"logger\":\"org.elasticsearch.flume\",\"level\":\"DEBUG\",\"timestamp\":1305075450270," +
                "\"threadName\":\"org.elasticsearch.flume.spring.scheduling.timer.ReschedulingTimerFactoryBean#2\",\"message\":\"Testing Json string creation\"," +
                "\"MDC\":{\"id\":\"123\",\"projectId\":\"334\"}}").getBytes(), 1, Priority.DEBUG, System.nanoTime(), "notlocalhost");
        sink.append(jsonEvent);

        sink.close();

        searchClient.admin().indices().refresh(refreshRequest("flume")).actionGet();

        assertBasicSearch(event);
        assertPrioritySearch(event);
        assertHostSearch(event);
        assertBodySearch(event);        
        assertFieldsSearch(event);
    }

    @Test
    public void validateErrorCount() throws IOException, InterruptedException {
        ElasticSearchSink sink = createAndOpenSink();

        EventImpl invalidJsonEvent1 = new EventImpl(("{\"host\":\"host.name\",\"logger\":\"org.elasticsearch.flume\",\"level\":\"DEBUG\",\"timestamp\":1305075450270," +
                "\"threadName\":\"org.elasticsearch.flume.spring.scheduling.timer.ReschedulingTimerFactoryBean#2\",\"message\":\"Testing Json string creation\"," +
                "\"MDC\":{\"id\":\"123\",\"projectId\":\"334\"}").getBytes(), 1, Priority.DEBUG, System.nanoTime(), "notlocalhost");
        sink.append(invalidJsonEvent1);
        sink.close();

        ReportEvent event = sink.getMetrics();
        long noOfFailedEvents = event.getLongMetric("NO_OF_FAILED_EVENTS");
        assertEquals("1 event should ",noOfFailedEvents,1L);

    }

    private ElasticSearchSink createAndOpenSink() throws IOException, InterruptedException {
        ElasticSearchSink sink = new ElasticSearchSink();
        sink.setLocalOnly(true);
        sink.open();
        return sink;
    }

    private void assertBasicSearch(Event event) {
        assertCorrectResponse(3, event, executeSearch(matchAllQuery()));
    }

    private void assertPrioritySearch(Event event) {
        assertCorrectResponse(1, event, executeSearch(queryString("priority:INFO")));
    }

    private void assertHostSearch(Event event) {
        assertCorrectResponse(1, event, executeSearch(queryString("host:localhost")));
    }

    private void assertBodySearch(Event event) {
        assertCorrectResponse(1, event, executeSearch(fieldQuery("message.text", "goes")));
    }
    
    private void assertFieldsSearch(Event event) {
        assertCorrectResponse(1, event, executeSearch(fieldQuery("fields.attr1", "quux")));
    }
    
    private SearchResponse executeSearch(XContentQueryBuilder query) {
        return searchClient.prepareSearch("flume")
                .setQuery(query)
                .execute()
                .actionGet();
    }
    
    private void assertCorrectResponse(int count, Event event, SearchResponse response) {
        SearchHits hits = response.getHits();

        assertEquals(count, hits.getTotalHits());

        SearchHit hit = hits.getAt(0);
        
        Map<String, Object> source = hit.getSource();

        assertEquals(event.getHost(), source.get("host"));
        assertEquals("1970-01-01T00:00:00.000Z", source.get("timestamp"));
        assertEquals(event.getPriority().name(), source.get("priority"));

        @SuppressWarnings("unchecked")
        Map<String, Object> message = (Map<String, Object>) source.get("message");
        assertEquals(new String(event.getBody()), message.get("text"));

        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) source.get("fields");

        assertEquals(new String(event.getAttrs().get("attr1")), fields.get("attr1"));
        assertEquals(new String(event.getAttrs().get("attr2")), fields.get("attr2"));
    }

}
