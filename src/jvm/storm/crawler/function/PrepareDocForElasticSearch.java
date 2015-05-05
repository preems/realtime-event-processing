package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.crawler.CrawlerConfig;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by Preetham MS on 5/5/15.
 */
public class PrepareDocForElasticSearch extends BaseFunction {

    private String esIndex;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndex = conf.get(CrawlerConfig.ELASTICSEARCH_INDEX_NAME).toString();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        String file = tuple.getString(0).replaceAll("\\s+","");
        String task = tuple.getString(1);
        String user = tuple.getString(2);
        String content = JSONObject.escape(tuple.getString(3));

        JSONObject json = new JSONObject();
        json.put("file",file);
        json.put("task",task);
        json.put("user",user);
        json.put("content",content);

        collector.emit(new Values(esIndex,task,file,json.toJSONString()));
    }
}
