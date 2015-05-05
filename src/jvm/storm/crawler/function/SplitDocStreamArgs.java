package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by preems on 5/5/15.
 */
public class SplitDocStreamArgs extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String str = tuple.getString(0);

        JSONParser parser=new JSONParser();
        try {
            Object obj = parser.parse(str);
            JSONObject jobj =  (JSONObject)(obj);
            collector.emit(new Values(jobj.get("filename"), jobj.get("task"), jobj.get("user"), jobj.get("content")));
        }
        catch (ParseException e) {
            e.printStackTrace();
            return;
        }

    }
}
