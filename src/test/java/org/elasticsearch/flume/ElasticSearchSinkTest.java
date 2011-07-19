package org.elasticsearch.flume;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryString;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;

public class ElasticSearchSinkTest {

    private Node searchNode;

    private Client searchClient;
    private static final String INDEX_TYPE = "testIndexType";
    private static final String INDEX_NAME = "flume";

    @Before
    public void startSearchNode() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "none")
                .put("node.local", "true")
                .put("http.enabled", false)
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
        attributes.put("attr3", new String("{\"key\":\"value\"}").getBytes());

        Event event = new EventImpl("message goes here".getBytes(), 0, Priority.INFO, System.nanoTime(),
                "localhost", attributes);

        sink.append(event);
        sink.append(new EventImpl("bleh foo baz bar".getBytes(), 1, Priority.WARN, System.nanoTime(), "notlocalhost"));
        sink.append(new EventImpl("{\"key\":\"value\"}".getBytes(), 2, Priority.DEBUG, System.nanoTime(), "jsonbody"));
        sink.append(new EventImpl("{\"key\":\"value\",\"complex\":{\"subkey\":\"subvalue\"}}".getBytes(), 3, Priority.DEBUG,
                System.nanoTime(), "complexjsonbody"));

        sink.close();

        searchClient.admin().indices().refresh(refreshRequest(INDEX_NAME)).actionGet();

        assertBasicSearch(event);
        assertPrioritySearch(event);
        assertHostSearch(event);
        assertBodySearch(event);
        assertFieldsSearch(event);
        assertJsonBody(event);
        assertComplexJsonBody(event);
    }

    @Test
    public void validateErrorCount() throws IOException, InterruptedException {
        ElasticSearchSink sink = createAndOpenSink();

        EventImpl invalidJsonEvent1 = new EventImpl("{ \"not json\" : no".getBytes(), 1, Priority.DEBUG, System.nanoTime(),
                "notlocalhost");
        sink.append(invalidJsonEvent1);
        sink.close();

        ReportEvent event = sink.getMetrics();
        long noOfFailedEvents = event.getLongMetric("NO_OF_FAILED_EVENTS");
        assertEquals("1 event should ", noOfFailedEvents, 1L);
    }

    @Test
    public void validateSinkIndexTypeConfiguration() throws IOException, InterruptedException {
        EventSink sink = createAndOpenSink("", "log", "");

        EventImpl event = new EventImpl("new index message".getBytes(), 1, Priority.WARN, System.nanoTime(), "notlocalhost");
        sink.append(event);
        sink.close();

        assertSimpleTest(INDEX_NAME, "log", 1);
    }

    @Test
    public void validateIndexNamePatternUsed() throws IOException, InterruptedException {
        ElasticSearchSink sink = createAndOpenSink("", "log", "test_%Y-%m-%d");

        sink.append(new EventImpl("new index message".getBytes(), 0, Priority.WARN, System.nanoTime(), "notlocalhost"));
        sink.append(new EventImpl("new index message".getBytes(), TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS), Priority.WARN,
                System.nanoTime(), "notlocalhost"));
        sink.close();

        assertSimpleTest("test_1970-01-01", "log", 1);
        assertSimpleTest("test_1970-01-02", "log", 1);
        assertSimpleTest(INDEX_NAME, "log", 2);
    }

    private void assertSimpleTest(String indexName, String indexType, int hits) {
        searchClient.admin().indices().refresh(refreshRequest(indexName)).actionGet();
        SearchResponse response = searchClient.prepareSearch(indexName).setTypes(indexType)
                .setQuery(fieldQuery("message.text", "new")).execute().actionGet();
        assertEquals("There should have been " + hits + " search result for default index type", hits, response.getHits()
                .getTotalHits());
    }

    private ElasticSearchSink createAndOpenSink() throws IOException, InterruptedException {
        return createAndOpenSink(INDEX_NAME, INDEX_TYPE, "");
    }

    private ElasticSearchSink createAndOpenSink(String indexName, String indexType, String indexPattern) throws IOException,
            InterruptedException {
        ElasticSearchSink sink = new ElasticSearchSink();
        sink.setLocalOnly(true);
        if (StringUtils.isNotBlank(indexName)) {
            sink.setIndexName(indexName);
        }
        if (StringUtils.isNotBlank(indexPattern)) {
            sink.setIndexPattern(indexPattern);
        }
        if (StringUtils.isNotBlank(indexType)) {
            sink.setIndexType(indexType);
        }
        sink.open();
        return sink;
    }

    private void assertBasicSearch(Event event) {
        assertCorrectResponse(4, event, executeSearch(matchAllQuery()));
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

    private void assertJsonBody(Event event) {
        SearchResponse response = executeSearch(queryString("host:jsonbody"));
        @SuppressWarnings("unchecked")
        Map<String, Object> json = (Map<String, Object>) response.getHits().getAt(0).getSource().get("message");
        assertEquals("value", json.get("key"));
    }

    @SuppressWarnings("unchecked")
    private void assertComplexJsonBody(Event event) {
        SearchResponse response = executeSearch(queryString("host:complexjsonbody"));

        Map<String, Object> json = (Map<String, Object>) response.getHits().getAt(0).getSource().get("message");
        assertEquals("value", json.get("key"));

        json = (Map<String, Object>) json.get("complex");
        assertEquals("subvalue", json.get("subkey"));
    }

    private SearchResponse executeSearch(QueryBuilder query) {
        return executeSearch(query, INDEX_NAME, INDEX_TYPE);
    }

    private SearchResponse executeSearch(QueryBuilder query, String indexName, String indexType) {
        return searchClient.prepareSearch(indexName).setTypes(indexType)
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
        @SuppressWarnings("unchecked")
        Map<String, Object> attr3 = (Map<String, Object>) fields.get("attr3");
        assertEquals("value", attr3.get("key"));
    }

}
