package storm.crawler.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Preetham MS on 4/27/15.
 */
public class SplitFunction extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        String str = tuple.getString(0);
        String[] list = str.split(" ");
        if (list.length==4) {
            collector.emit(new Values(list[0],list[1],list[2],list[3]));
        }
    }
}
