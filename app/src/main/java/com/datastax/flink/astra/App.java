package com.datastax.flink.astra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String ASTRA_SCB = "astra.scb";
    private static final String ASTRA_CLIENTID = "astra.clientid";
    private static final String ASTRA_SECRET = "astra.secret";

    private static Cluster buildCluster(Cluster.Builder builder, Properties props) {

        return builder
                .withCloudSecureConnectBundle(ClassLoader.getSystemClassLoader().getResourceAsStream(props.getProperty(ASTRA_SCB)))
                .withAuthProvider(
                        new PlainTextAuthProvider(
                                props.getProperty(ASTRA_CLIENTID),
                                props.getProperty(ASTRA_SECRET)))
                .build();
    }

    private static void setupSchema(Properties props) {

        Cluster cluster = buildCluster(Cluster.builder(), props);
        Session session = cluster.connect();
        session.execute("drop table if exists example.wordcount");
        session.execute("CREATE TABLE IF NOT EXISTS example.wordcount (\n" +
                "word text,\n" +
                "count bigint,\n" +
                "PRIMARY KEY(word))\n");
        session.close();
        cluster.close();
    }

    // Most of the code below stems from a combination of
    // https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/cassandra/#cassandra-sink-example-for-streaming-tuple-data-type
    // and
    // https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java
    private static void runFlink(Properties props)
    throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements("the quick brown fox", "jumped over", "the lazy dog", "foxes are just", "lazier than dogs", "or at least our dog");

        DataStream<Tuple2<String, Long>> result = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                        // normalize and split the line
                        String[] words = value.toLowerCase().split("\\s");

                        // emit the pairs
                        for (String word : words) {
                            //Do not accept empty word, since word is defined as primary key in C* table
                            if (!word.isEmpty()) {
                                out.collect(new Tuple2<String, Long>(word, 1L));
                            }
                        }
                    }
                })
                .keyBy(value -> value.f0)
                // The method call below is also included in the current sample documentation at
                // https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/datastream/cassandra/
                // However, including this time window causes no data to be recorded, so for now we'll just
                // remove it (since we're after something that can be used to demonstrate connectivity rather
                // than anything related to time windowing).
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        CassandraSink.addSink(result)
                .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
                // We cannot use setHost() and setClusterBuilder() together; the builder will
                // (correctly) throw an exception in that case
                //.setHost("127.0.0.1")
                //
                // Regrettably ClusterBuilder is an abstract class rather than interface so
                // we can't swap in a closure here; we have to use the anonymous inner class
                // (or something else equivalent).
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return App.buildCluster(builder, props);
                    }
                })
                .setFailureHandler((failure) -> { log.error("Exception in C* ops", failure); })
                .build();

        result.print().setParallelism(1);

        env.execute("Flink Astra Test");
    }

    public static void main(String[] args) throws Exception {

        // For demonstration purposes we load Astra creds + SCB location from a properties file
        Properties props = new Properties();
        props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("app.properties"));

        setupSchema(props);
        runFlink(props);
    }
}
