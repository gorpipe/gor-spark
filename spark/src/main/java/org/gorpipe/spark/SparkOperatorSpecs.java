package org.gorpipe.spark;

import gorsat.Commands.CommandParseUtilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkOperatorSpecs {
    Map<String,Object> configMap = new HashMap<>();

    public void addConfig(String config,Object value) {
        String[] split = CommandParseUtilities.quoteSafeSplit(config,'.');
        Map<String,Object> current = configMap;
        for(int i = 0; i < split.length-1; i++) {
            String s = split[i];
            if(s.startsWith("\"")) s = s.substring(1,s.length()-1);
            if(current.containsKey(s)) {
                current = (Map<String,Object>)current.get(s);
            } else {
                Map<String,Object> next = new HashMap<>();
                current.put(s, next);
                current = next;
            }
        }
        String key = split[split.length-1];
        if(key.startsWith("\"")) key = key.substring(1,key.length()-1);
        current.put(key,value);
    }

    private Map<String,Object> merge(Map<String,Object> body,Map<String,Object> config) {
        for(String key : config.keySet()) {
            body.merge(key,config.get(key),(m1,m2) -> {
                if(m1 instanceof Map && m2 instanceof Map) {
                    Map<String,Object> map1 = (Map)m1;
                    Map<String,Object> map2 = (Map)m2;
                    return merge(map1,map2);
                } else if(m1 instanceof List && m2 instanceof List) {
                    List<String> list1 = (List)m1;
                    List<String> list2 = (List)m2;
                    var ret = new ArrayList<>(list1);
                    ret.addAll(list2);
                    return ret;
                }
                return m2;
            });
        }
        return body;
    }

    void addArray(String[] resources) {
        for (String config : resources) {
            var i = config.indexOf('=');
            if (i > 0) {
                var key = config.substring(0,i).trim();
                var value = config.substring(i+1).trim();
                if (value.startsWith("'")) {
                    addConfig(key, value.substring(1,value.length()-1));
                } else {
                    try {
                        Integer ii = Integer.parseInt(value);
                        addConfig(key, ii);
                    } catch (NumberFormatException ne) {
                        try {
                            Double dd = Double.parseDouble(value);
                            addConfig(key, dd);
                        } catch(NumberFormatException nfe) {
                            addConfig(key, value);
                        }
                    }
                }
            }
        }
    }

    public void addVolumeClaim(String pod, String name, String pvc,String path,String subPath,boolean readOnly) {
        String baseConfig = "spec.sparkConf.\"spark.kubernetes."+pod+".volumes.persistentVolumeClaim."+name;
        String claimConfig = baseConfig+".options.claimName\"";
        addConfig(claimConfig,pvc);
        addMount(baseConfig,path,subPath,readOnly);
    }

    public void addHostPath(String pod,String name,String hostPath,String path,String subPath,boolean readOnly) {
        String baseConfig = "spec.sparkConf.\"spark.kubernetes."+pod+".volumes.hostPath."+name;
        String hostPathConfig = baseConfig+".options.path\"";
        addConfig(hostPathConfig,hostPath);
        addMount(baseConfig,path,subPath,readOnly);
    }

    public void addMount(String baseConfig, String path, String subPath, boolean readOnly) {
        String pathConfig = baseConfig+".mount.path\"";
        addConfig(pathConfig,path);
        if(subPath!=null) {
            String subPathConfig = baseConfig + ".mount.subPath\"";
            addConfig(subPathConfig, subPath);
        }
        if(readOnly) {
            String readOnlyConfig = baseConfig+".mount.readOnly\"";
            addConfig(readOnlyConfig,readOnly);
        }
    }

    public void addDriverVolumeClaim(String name,String pvc,String path,String subPath,boolean readOnly) {
        addVolumeClaim("driver",name,pvc,path,subPath,readOnly);
    }

    public void addExecutorVolumeClaim(String name,String pvc,String path,String subPath,boolean readOnly) {
        addVolumeClaim("executor",name,pvc,path,subPath,readOnly);
    }

    public void addDriverHostPath(String name,String pvc,String path,String subPath,boolean readOnly) {
        addHostPath("driver",name,pvc,path,subPath,readOnly);
    }

    public void addExecutorHostPath(String name,String pvc,String path,String subPath,boolean readOnly) {
        addHostPath("executor",name,pvc,path,subPath,readOnly);
    }

    public void apply(Map<String,Object> body) {
        merge(body,configMap);
    }
}
