package org.gorpipe.spark;

import java.util.HashMap;
import java.util.Map;

public class SparkOperatorSpecs {
    Map<String,Object> configMap = new HashMap<>();

    public void addConfig(String config,Object value) {
        String[] split = config.split("\\.");
        Map<String,Object> current = configMap;
        for(int i = 0; i < split.length-1; i++) {
            String s = split[i];
            if(current.containsKey(s)) {
                current = (Map<String,Object>)current.get(s);
            } else {
                Map<String,Object> next = new HashMap<>();
                current.put(s, next);
                current = next;
            }
        }
        current.put(split[split.length-1],value);
    }

    private Map<String,Object> merge(Map<String,Object> body,Map<String,Object> config) {
        for(String key : config.keySet()) {
            body.merge(key,config.get(key),(m1,m2) -> {
                if(m1 instanceof Map && m2 instanceof Map) {
                    Map<String,Object> map1 = (Map)m1;
                    Map<String,Object> map2 = (Map)m2;
                    return merge(map1,map2);
                }
                return m2;
            });
        }
        return body;
    }

    public void apply(Map<String,Object> body) {
        merge(body,configMap);
    }
}
