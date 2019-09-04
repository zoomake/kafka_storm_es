package com.grow;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.LATEST;

public class KafkaStormSample {

    protected static KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    private static Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    };

    public static void main(String[] args) throws TException {
        Config config = new Config();
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig.builder("47.103.19.138:9092",
                "stormtest")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200).setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
                .setRetry(newRetryService()).setOffsetCommitPeriodMs(10000).setFirstPollOffsetStrategy(LATEST)
                .setMaxUncommittedOffsets(250).build();

        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<String, String>(kafkaSpoutConfig); // KafkaSpout实现

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", kafkaSpout,2).setNumTasks(4);
        builder.setBolt("word-spitter", new SplitBolt(),2).setNumTasks(4).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt(),1).setNumTasks(1).shuffleGrouping("word-spitter");

//        Map<Object, Object> boltConf = new HashMap<Object, Object>();
//        boltConf.put("es.nodes", "47.103.19.138:9200");
//        boltConf.put("es.index.auto.create", "true");
//        boltConf.put("es.ser.writer.bytes.class", "org.elasticsearch.storm.serialization.StormTupleBytesConverter");
//        boltConf.put("es.input.json", "true");

        EsConfig esConfig = new EsConfig(new String[]{"http://47.103.19.138:9200"}); // 定义一个ES的配置信息
        EsTupleMapper esTupleMapper = new DefaultEsTupleMapper(); // 定义ES的默认映射
        MyEsIndexBolt indexBolt = new MyEsIndexBolt(esConfig, esTupleMapper); //定义一个索引Bolt

//      builder.setBolt("storm-es-bolt", new EsBolt("storm/docs", boltConf))
        builder.setBolt("storm-es-bolt", indexBolt)
                .fieldsGrouping("word-counter", new Fields("source", "index", "type", "id"));


//        EsConfig esConfig = new EsConfig(new String[]{"http://47.103.19.138:9200"}); // 定义一个ES的配置信息
//        EsTupleMapper esTupleMapper = new DefaultEsTupleMapper(); // 定义ES的默认映射
//        EsIndexBolt indexBolt = new EsIndexBolt(esConfig, esTupleMapper); //定义一个索引Bolt
//
//        builder.setBolt("es-bolt", indexBolt, 1).fieldsGrouping("word-counter",new Fields("source", "index", "type", "id")); // 向topology注入indexBolt以处理kafka-bolt的数据
//        builder.setBolt("es-bolt", indexBolt, 1).fieldsGrouping("word-counter",new Fields("index", "type", "id", "source")); // 向topology注入indexBolt以处理kafka-bolt的数据

        // idea提交任务，读取本地Storm-core配置文件，提交集群
        // 读取本地storm-core包下的storm.yaml配置
        Map stormConf = Utils.readStormConfig();
        // 读取classpath下的配置文件
        // Map stormConf = Utils.findAndReadConfigFile("storm.yaml");
        List<String> seeds = new ArrayList<String>();
        seeds.add("47.103.19.138");
        stormConf.put(Config.NIMBUS_SEEDS, seeds);
        stormConf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        stormConf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        stormConf.putAll(config);
        System.out.println(stormConf);

        // 提交集群运行的jar
        String inputJar = "D:\\idea\\kafka_storm_es\\out\\artifacts\\storm_kafka_jar\\storm_kafka.jar";

        String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
        String jsonConf = JSONValue.toJSONString(stormConf);

        NimbusClient nimbus = NimbusClient.getConfiguredClient(stormConf);
        Nimbus.Iface client = nimbus.getClient();
        client.submitTopology("storm-kafka", uploadedJarLocation, jsonConf, builder.createTopology());

        Long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 3000 * 10 * 60L) {

        }

        client.killTopology("storm-kafka");
    }
}
