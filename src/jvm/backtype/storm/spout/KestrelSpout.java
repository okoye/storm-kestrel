package backtype.storm.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.io.*;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import net.lag.kestrel.thrift.Item;
import net.lag.kestrel.ThriftClient;


/**
 * This spout can be used to consume messages in a reliable way from a cluster 
 * of Kestrel servers. It is recommended that you set the parallelism hint to a
 * multiple of the number of Kestrel servers, otherwise the read load will be
 * higher on some Kestrel servers than others.
 */
public class KestrelSpout implements IRichSpout {
    public static Logger LOG = Logger.getLogger(KestrelSpout.class);

    public static final long BLACKLIST_TIME_MS = 1000 * 60;
    
    
    private List<String> _hosts = null;
    private int _port = -1;
    private String _queueName = null;
    private SpoutOutputCollector _collector;
    private Scheme _scheme;

    private List<KestrelClientInfo> _kestrels;
    private int _emitIndex;
    
    private static class KestrelSourceId {
        public KestrelSourceId(int index, int id) {
            this.index = index;
            this.id = id;
        }
        
        int index;
        int id;
    }
    
    private static class KestrelClientInfo {
        public ThriftClient client;
        public Long blacklistTillTimeMs;
        public String host;
        public int port;
        
        public KestrelClientInfo(String host, int port) {
            this.host = host;
            this.port = port;
            this.blacklistTillTimeMs = 0L;
            this.client  = null;
        }
    }
    
    public KestrelSpout(List<String> hosts, int port, String queueName, Scheme scheme) {
        if(hosts.isEmpty()) {
            throw new IllegalArgumentException("Must configure at least one host");
        }
        _port = port;
        _hosts = hosts;
        _queueName = queueName;
        _scheme = scheme;        
    }
    
    public KestrelSpout(String hostname, int port, String queueName, Scheme scheme) {
        this(Arrays.asList(hostname), port, queueName, scheme);
    }

    public KestrelSpout(String hostname, int port, String queueName) {
        this(hostname, port, queueName, new RawScheme());
    }
    
    public KestrelSpout(List<String> hosts, int port, String queueName) {
        this(hosts, port, queueName, new RawScheme());
    }

    public Fields getOutputFields() {
       return _scheme.getOutputFields();
    }
    
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _emitIndex = 0;
        _kestrels = new ArrayList<KestrelClientInfo>();
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int myIndex = context.getThisTaskIndex();
        int numHosts = _hosts.size();
        if(numTasks < numHosts) {
            for(String host: _hosts) {
                _kestrels.add(new KestrelClientInfo(host, _port));
            }
        } else {
            String host = _hosts.get(myIndex % numHosts);
            _kestrels.add(new KestrelClientInfo(host, _port));
        }       
    }

    public void close() {
        for(KestrelClientInfo info: _kestrels) {
            if(info.client!=null) {
                info.client.close();
            }
        }
        _kestrels.clear();
    }

    public void nextTuple() {
        long now = System.currentTimeMillis();
        boolean success = false;
        for(int i=0; i<_kestrels.size(); i++) {
            int index = (_emitIndex + i) % _kestrels.size();
            KestrelClientInfo info = _kestrels.get(index);
            if(now > info.blacklistTillTimeMs) {
                try {
                    if(info.client==null) {
                        info.client = new ThriftClient(info.host, info.port);
                    }
                    List<Item> items = info.client.get(_queueName, 1, 0, false);

                    if(!items.isEmpty()) {
                        assert items.size() == 1;
                        Item item = items.get(0);
                        List<Object> tuple = _scheme.deserialize(item.get_data());
                        _collector.emit(tuple, new KestrelSourceId(index, item.get_xid()));
                        success = true;
                        _emitIndex = index;
                        break;
                    }
                } catch(TException e) {
                    blacklist(info, e);
                    
                }
            }            
        }
        _emitIndex = (_emitIndex + 1) % _kestrels.size();
        if(!success) {
            Utils.sleep(10);
        }
    }
    
    private void blacklist(KestrelClientInfo info, Throwable t) {
        LOG.warn("Failed to read from Kestrel at " + info.host + ":" + info.port, t);
        //this case can happen when it fails to connect to Kestrel (and so never stores the connection)
        if(info.client!=null) {
            info.client.close();
        }
        info.client = null;
        info.blacklistTillTimeMs = System.currentTimeMillis() + BLACKLIST_TIME_MS;
    }

    public void ack(Object msgId) {
        KestrelSourceId sourceId = (KestrelSourceId) msgId;
        KestrelClientInfo info = _kestrels.get(sourceId.index);
        
        //if the transaction didn't exist, it just returns false. so this code works
        //even if client gets blacklisted, disconnects, and kestrel puts the item 
        //back on the queue
        try {
            if(info.client!=null) {
                HashSet xids = new HashSet();
                xids.add(sourceId.id);
                info.client.confirm(_queueName, xids);
            }
        } catch(TException e) {
            blacklist(info, e);            
        }
    }
    
    public void fail(Object msgId) {
        KestrelSourceId sourceId = (KestrelSourceId) msgId;
        KestrelClientInfo info = _kestrels.get(sourceId.index);
        
        // see not above about why this works with blacklisting strategy
        try {
            if(info.client!=null) {
                HashSet xids = new HashSet();
                xids.add(sourceId.id);
                info.client.abort(_queueName, xids);
            }
        } catch(TException e) {
            blacklist(info, e);            
        }
    }

    public boolean isDistributed() {
        return true;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(getOutputFields());
    }
}

