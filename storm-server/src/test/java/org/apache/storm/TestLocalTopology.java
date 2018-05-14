package org.apache.storm;

import org.apache.storm.daemon.nimbus.Nimbus;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TestLocalTopology {

    public static class RandomSentenceSpout extends BaseRichSpout {
        private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);

        SpoutOutputCollector _collector;
        Random _rand;


        @Override
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
                    sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")};
            final String sentence = sentences[_rand.nextInt(sentences.length)];

            LOG.debug("Emitting tuple: {}", sentence);

            _collector.emit(new Values(sentence));
        }

        protected String sentence(String input) {
            return input;
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String sentence = tuple.getString(0);
            int count = sentence.split(" ").length;
            System.out.println(count);
            collector.emit(new Values(count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("count"));
        }
    }

    @Test
    public void run() throws Exception {

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 2);
        builder.setBolt("count", new WordCountBolt(), 4).shuffleGrouping("spout");

        conf.setDebug(true);

        String topologyName = "word-count";

        conf.setNumAckers(0);
        conf.setNumWorkers(2);
        conf.setTopologyWorkerMaxHeapSize(768);
        conf.setWorkerMaxBandwidthMbps(20);

        cluster.submitTopology(topologyName, conf, builder.createTopology());

        Thread.sleep(30000000);

        cluster.shutdown();

    }
}
