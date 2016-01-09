package resa.evaluation.topology.tomVLD;

import backtype.storm.Config;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Config manager provides methods to load a storm config from a file and access its content.
 */
public class StormConfigManager {
    /**
     * Read config from file path
     * @param path - the path to the file containing yaml config
     * @return the Config for this file
     * @throws FileNotFoundException
     */
    public static Config readConfig(String path) throws FileNotFoundException {
        Config config = new Config();
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(path));
        HashMap<String, Object> map = (HashMap<String, Object>)yaml.load(inputStream);
        config.putAll(map);
        return config;
    }
    public static int getInt(Map<String, Object> config, String key) {
        Object obj = config.get(key);
        if (obj instanceof Integer)
            return (Integer)obj;
        return ((Long)config.get(key)).intValue();
    }
    public static String getString(Map<String, Object> config, String key) {
        return (String)config.get(key);
    }

    public static String getString(Map<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        if (value != null && value instanceof String) {
            return (String)value;
        }
        return defaultValue;
    }

    public static List<String> getListOfStrings(Map<String, Object> config, String key) {
        return (List<String>)config.get(key);
    }
}
