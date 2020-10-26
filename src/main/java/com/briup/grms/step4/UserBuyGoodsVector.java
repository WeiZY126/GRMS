package com.briup.bigdata.project.grms.step4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class UserBuyGoodsVector {

    public static class UserBuyGoodsVectorMap extends Mapper<LongWritable, Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[1]),new Text(split[0]+":"+split[2]));
        }
    }

    public static class UserBuyGoodsVectorReduce extends Reducer<Text, Text,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map = new HashMap<>();
            for (Text value1 : values) {
                String[] value = value1.toString().split("[:]");
                if (!map.containsKey(value[0])){
                    map.put(value[0],Integer.parseInt(value[1]));
                }else{
                    map.replace(value[0],map.get(value[0])+Integer.parseInt(value[1]));
                }
            }
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            String s = "";
            for (Map.Entry<String, Integer> entry : entries) {
                s=s+","+entry.getKey()+":"+entry.getValue();
            }
            context.write(key,new Text(s.substring(1,s.length())));
        }
    }
}