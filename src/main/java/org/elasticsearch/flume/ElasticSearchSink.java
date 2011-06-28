package org.elasticsearch.flume;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Pair;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchSink.class);
    private static final String DEFAULT_INDEX_NAME = "flume";
    private static final String DEFAULT_LOG_TYPE = "log";
    private static final int DEFAULT_ELASTICSEARCH_PORT = 9300;

    private Node node;
    private Client client;
    private String indexName = DEFAULT_INDEX_NAME;
    private String indexPattern = null;
    private String indexType =  DEFAULT_LOG_TYPE;

    private Charset charset = Charset.defaultCharset();


    private String[] hostNames = new String[0];
    private String clusterName = ClusterName.DEFAULT.value();

    // Enabled only for testing
    private boolean localOnly = false;

    private AtomicLong eventErrorCount = new AtomicLong(0L);
    private static final String NO_OF_FAILED_EVENTS = "NO_OF_FAILED_EVENTS";

    @Override
    public void append(Event e) throws IOException {
        // TODO strategize the name of the index, so that logs based on day can go to individula indexes, allowing simple cleanup by deleting older days indexes in ES
        XContentParser parser = null;
        byte[] data = e.getBody();
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .field("timestamp", new Date(e.getTimestamp()))
                    .field("host", e.getHost())
                    .field("priority", e.getPriority().name());

            builder.startObject("message");
            addField(builder, "text", data);
            builder.endObject();

            builder.startObject("fields");
            for (Map.Entry<String, byte[]> entry : e.getAttrs().entrySet()) {
                addField(builder, entry.getKey(), entry.getValue());
            }
            builder.endObject();
            
            index(e, builder);
        } catch (Exception ex) {
            LOG.error(String.format("Error Processing event: %s", new String(data)),ex);
            eventErrorCount.incrementAndGet();
        } finally {
            if (parser != null) parser.close();
        }
    }

    @Override
    public synchronized ReportEvent getMetrics() { 
        ReportEvent event = new ReportEvent("ElasticSearchSink");
        event.setLongMetric(NO_OF_FAILED_EVENTS,eventErrorCount.longValue());
        return event;
    }

    private void index(Event e, XContentBuilder builder) {
        String iName = indexName;
        if (indexPattern != null) {
            iName = e.escapeString(indexPattern);
        }
        client.prepareIndex(iName, indexType, null)
                .setSource(builder)
                .execute()
                .actionGet();

        if (!iName.equals(indexName)) {
            client.admin().indices().prepareAliases().addAlias(iName, indexName).execute().actionGet();
        }
    }
    
    private void addField(XContentBuilder builder, String fieldName, byte[] data) throws IOException {
        XContentParser parser = null;
        LOG.info(String.format("field: %s, data:%s", fieldName, new String(data)));
        try {
            XContentType contentType = XContentFactory.xContentType(data);
            if (contentType == null) {
                builder.field(fieldName, new String(data, charset));
            } else {
                parser = XContentFactory.xContent(contentType).createParser(data);
                parser.nextToken();
                builder.field(fieldName);
                builder.copyCurrentStructure(parser);
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        super.close();

        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }

    @Override
    public void open() throws IOException, InterruptedException {
        super.open();

        if (hostNames.length == 0) {
            LOG.info("Using ES AutoDiscovery mode");
            node = nodeBuilder().client(true).clusterName(clusterName).local(localOnly).node();
            client = node.client();
        } else {
            LOG.info("Using provided ES hostnames: " + Arrays.toString(hostNames));
            
            Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();
                
            TransportClient transportClient = new TransportClient(settings);
            for (String esHostName : hostNames) {
                LOG.info("Adding TransportClient: " + esHostName);
                transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(esHostName, DEFAULT_ELASTICSEARCH_PORT));
            }
            client = transportClient;
        }

    }


    /**
     * This is a special function used by the SourceFactory to pull in this class
     * as a plugin sink.
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders =
                new ArrayList<Pair<String, SinkBuilder>>();

        builders.add(new Pair<String, SinkBuilder>("elasticSearchSink", new ElasticSearchSinkBuilder()));
        return builders;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexPattern() {
        return indexPattern;
    }
    
    public void setIndexPattern(String indexPattern) {
        this.indexPattern = indexPattern;
    }
    
    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }
    
    public void setHostNames(String[] hostNames) {
        this.hostNames = hostNames;
    }

    public String[] getHostNames() {
        return hostNames;
    }

    void setLocalOnly(boolean localOnly) {
        this.localOnly = localOnly;
    }

    boolean isLocalOnly() {
        return localOnly;
    }
}
