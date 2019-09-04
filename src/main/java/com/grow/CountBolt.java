package com.grow;

import kafka.utils.Json;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CountBolt implements IRichBolt {
    Map<String, Integer> counters;
    JSONObject result = new JSONObject();
    private OutputCollector collector;
    String msgId = UUID.randomUUID().toString();


    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counters = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        if (!counters.containsKey(str)){
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        result.put("result",JSONObject.toJSONString(counters));

        Values values = new Values(result.toString(), "storm", "docs",msgId);

        collector.emit(values);

        System.out.println("Count:" + JSONObject.toJSONString(counters));
        System.out.println("ID:" + msgId);
//        collector.ack(tuple);
    }

    public void cleanup() {
//        for (Map.Entry<String, Integer> entry : counters.entrySet()){
//            System.out.println(entry.getKey() + ":" + entry.getValue());
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source", "index", "type", "id"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
