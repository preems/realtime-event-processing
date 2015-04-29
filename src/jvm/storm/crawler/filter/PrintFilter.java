package storm.crawler.filter;


import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Sunil Kalmadka on 4/5/2015.
 */

public class PrintFilter  extends BaseFilter {

    public String prefix;

    public PrintFilter(String prefix) {
        this.prefix=prefix;
    }

    public PrintFilter(){
        this.prefix="";
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println("PrintFilter ["+this.prefix+"]: "+tridentTuple);
        return true;
    }
}

