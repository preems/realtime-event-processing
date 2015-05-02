package storm.crawler.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 5/1/15.
 */
public class SplitDRPCArgs extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String argsString = tridentTuple.getString(0);
        String[] argsSplit = argsString.split(" ");

        tridentCollector.emit(new Values(argsSplit[0], argsSplit[1]));
    }
}