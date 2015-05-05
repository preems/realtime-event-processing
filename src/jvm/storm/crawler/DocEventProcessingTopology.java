package storm.crawler;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import org.json.simple.JSONObject;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import storm.crawler.filter.KafkaProducerFilter;
import storm.crawler.filter.PrintFilter;
import storm.crawler.filter.URLFilter;
import storm.crawler.function.*;
import storm.crawler.common.ConfigReader;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import storm.crawler.state.ESTridentTupleMapper;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

import java.io.*;
import java.lang.System;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Preetham MS on 5/5/15.
 */
public class DocEventProcessingTopology {

    public static StormTopology buildTopology(Config conf, LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts(conf.get(CrawlerConfig.KAFKA_CONSUMER_HOST_NAME) + ":" +conf.get(CrawlerConfig.KAFKA_CONSUMER_HOST_PORT));
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zk, (String) conf.get(CrawlerConfig.KAFKA_TOPIC_DOCUMENT_NAME));
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(kafkaConfig);

        //ElasticSearch Persistent State
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", conf.get(CrawlerConfig.ELASTICSEARCH_CLUSTER_NAME))
                .put("storm.elasticsearch.hosts", conf.get(CrawlerConfig.ELASTICSEARCH_HOST_NAME) + ":" + conf.get(CrawlerConfig.ELASTICSEARCH_HOST_PORT))
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<JSONObject>(new ClientFactory.NodeClient(esSettings.getAsMap()), JSONObject.class);
        TridentState esStaticState = topology.newStaticState(esStateFactory);

        String esIndex = (String)(conf.get(CrawlerConfig.ELASTICSEARCH_INDEX_NAME));
        topology.newStream("docstream",spout)
                .each( new Fields("str"), new SplitDocStreamArgs(), new Fields("filename", "task", "user", "content"))
                .each( new Fields("filename", "task", "user"), new PrintFilter("Kafka"))
                .each( new Fields("filename","task","user","content"), new PrepareDocForElasticSearch(), new Fields("index","type","id","source") )
                .partitionPersist(esStateFactory, new Fields("index","type","id","source"), new ESIndexUpdater<String>(new ESTridentTupleMapper()), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("[ERROR] Configuration File Required");
        }
        Config conf = new Config();

        //Map topologyConfig = readConfigFile(args[0]);
        //conf.putAll(topologyConfig);

        // Store all the configuration in the Storm conf object
        conf.putAll(ConfigReader.readConfigFile(args[0]));

        //Second arg should be local in order to run locally
        if(args[1].equals("local"))
        {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster localcluster = new LocalCluster();
            localcluster.submitTopology("doc_event_processing",conf,buildTopology(conf, drpc));

            String searchQuery = "HoloLens crawl_test";
            System.out.println("---* Result: " + drpc.execute("search",  searchQuery));
        }
        else
        {
            StormSubmitter.submitTopologyWithProgressBar("doc_event_processing", conf, buildTopology(conf, null));
        }
    }
}
