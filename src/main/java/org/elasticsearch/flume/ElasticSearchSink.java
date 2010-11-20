package org.elasticsearch.flume;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Pair;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticSearchSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchSink.class);

    private Node node;
    private Client client;
    private String indexName = "flume";
    private static final String LOG_TYPE = "LOG";
    private Charset charset = Charset.defaultCharset();
    private final String[] esHostNames;
    private static final int DEFAULT_ELASTICSEARCH_PORT = 9300;


    public ElasticSearchSink(String esHostNames) {
        LOG.info("ES hostnames: " + esHostNames);
        if (esHostNames == null || esHostNames.trim().length()==0) {
            this.esHostNames = new String[0];
        } else {
            this.esHostNames = esHostNames.split(",");
        }
    }

    @Override
    public void append(Event e) throws IOException {

        // TODO strategize the name of the index, so that logs based on day can go to individula indexes, allowing simple cleanup by deleting older days indexes in ES
        IndexResponse response = client.prepareIndex(indexName, LOG_TYPE, null)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("message", new String(e.getBody(), charset))
                        .field("timestamp", new Date(e.getTimestamp()))
                        .field("host", e.getHost())
                        .field("priority", e.getPriority().name())
                        // TODO add attributes
                        .endObject()
                )
                .execute()
                .actionGet();
    }


    @Override
    public void close() throws IOException {
        super.close();

        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }

    @Override
    public void open() throws IOException {
        super.open();

        if (esHostNames.length == 0) {
            LOG.info("Using ES AutoDiscovery mode");
            node = nodeBuilder().client(true).node();
            client = node.client();
        } else {
            LOG.info("Using provided ES hostnames: " + esHostNames.length);
            TransportClient transportClient = new TransportClient();
            for (String esHostName : esHostNames) {
                LOG.info("Adding TransportClient: " + esHostName);
                transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(esHostName, DEFAULT_ELASTICSEARCH_PORT));
            }
            client = transportClient;
        }

    }


    public static SinkBuilder builder() {

        return new SinkBuilder() {
            @Override
            public EventSink build(Context context, String... argv) {
                if (argv.length == 0
                        || argv.length == 1) {
                    String esHostNames = "";
                    if (argv.length == 1) {
                        esHostNames = argv[0];
                    }
                    return new ElasticSearchSink(esHostNames);
                } else {
                    throw new IllegalArgumentException(
                            "usage: elasticSearchSink[([esHostNames])]");
                }


            }
        };
    }

    /**
     * This is a special function used by the SourceFactory to pull in this class
     * as a plugin sink.
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders =
                new ArrayList<Pair<String, SinkBuilder>>();
        builders.add(new Pair<String, SinkBuilder>("elasticSearchSink", builder()));
        return builders;
    }

}
