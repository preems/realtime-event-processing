package storm.crawler.common;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Preetham MS on 5/5/15.
 */
public class ConfigReader {

    public static Map readConfigFile(String filename) throws IOException {
        Map ret;
        Yaml yaml = new Yaml(new SafeConstructor());
        InputStream inputStream = new FileInputStream(new File(filename));

        try {
            ret = (Map)yaml.load(inputStream);
        } finally {
            inputStream.close();
        }

        if(ret == null) {
            ret = new HashMap();
        }

        return new HashMap(ret);
    }
}
