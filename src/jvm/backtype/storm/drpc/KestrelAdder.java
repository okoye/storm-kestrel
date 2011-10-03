package backtype.storm.drpc;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.json.simple.JSONValue;
import net.lag.kestrel.ThriftClient;
import net.lag.kestrel.thrift.Item;
import org.apache.thrift.TException;


public class KestrelAdder implements SpoutAdder {
    ThriftClient _client;

    public KestrelAdder(String host, String port) {
        try {
            _client = new ThriftClient(host, Integer.parseInt(port));
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public void add(String function, String jsonArgs, String returnInfo) {
        Map val = new HashMap();
        val.put("args", jsonArgs);
        val.put("return", returnInfo);
        try {
            List<byte[]> items = new ArrayList<byte[]>();
            items.add(JSONValue.toJSONString(val).getBytes());
            _client.put(function, items, 0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
