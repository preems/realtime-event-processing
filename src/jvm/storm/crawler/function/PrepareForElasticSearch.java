package storm.crawler.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrepareForElasticSearch extends BaseFunction {

    private String esIndex;

    public PrepareForElasticSearch(String str) {
        //The index of the ElasticSearch
        this.esIndex=str;
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        // Escape the contents so that they can be send in a JSON
        String url = JSONObject.escape(tridentTuple.getString(0));
        String content_html = JSONObject.escape(tridentTuple.getString(1));
        String title = JSONObject.escape(tridentTuple.getString(2));
        String task = JSONObject.escape(tridentTuple.getString(3));
        String user = JSONObject.escape(tridentTuple.getString(2));

        // Create the JSON to be stored in ElasticSearch
        JSONObject json = new JSONObject();
        json.put("url",url);
        json.put("content",content_html);
        json.put("title",title);
        json.put("task",task);
        json.put("user",user);

        //String source = "{\"url\":\""+url+"\", \"content\":\""+content_html+"\", \"title\":\""+title+"\"}";
//        System.out.println("----- PrepareForElasticSearch: id = "+url);
//        System.out.println("----- PrepareForElasticSearch: source = "+source);

        //Insert into a 'task' type
        //Use url as id in ES
        tridentCollector.emit(new Values(this.esIndex, task, url ,json.toJSONString()));
    }
}