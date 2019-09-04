//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.grow;


import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.Args;
import org.apache.storm.elasticsearch.bolt.AbstractEsBolt;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MyEsIndexBolt extends AbstractEsBolt {
    private final EsTupleMapper tupleMapper;

    public MyEsIndexBolt(EsConfig esConfig) {
        this(esConfig, new DefaultEsTupleMapper());
    }

    public MyEsIndexBolt(EsConfig esConfig, EsTupleMapper tupleMapper) {
        super(esConfig);
        this.tupleMapper = (EsTupleMapper) Objects.requireNonNull(tupleMapper);
    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
    }

    static String getEndpoint(String index, String type, String id) {
        Objects.requireNonNull(index);
        Args.notBlank(index, "index");
        StringBuilder sb = new StringBuilder();
        sb.append("/").append(index);
        if (type != null && !type.isEmpty()) {
            sb.append("/").append(type);
        }

        if (id != null && !id.isEmpty()) {
            sb.append("/").append(id);
        }

        return sb.toString();
    }

    public void process(Tuple tuple) {
        try {
            String source = this.tupleMapper.getSource(tuple);
            String index = this.tupleMapper.getIndex(tuple);
            String type = this.tupleMapper.getType(tuple);
            String id = this.tupleMapper.getId(tuple);
            Map<String, String> params = this.tupleMapper.getParams(tuple, new HashMap());
            HttpEntity entity = new StringEntity(source, ContentType.APPLICATION_JSON);
            client.performRequest("put", getEndpoint(index, type, id), params, entity, new Header[0]);
            this.collector.ack(tuple);
        } catch (Exception var7) {
            this.collector.reportError(var7);
            this.collector.fail(tuple);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
