package com.briup.bigdata.project.grms.step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GoodsCooccurrenceMatrix {

    public static class GoodsCooccurrenceMatrixMap extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }

    public static class GoodsCooccurrenceMatrixReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map = new HashMap<>();
            for (Text value : values) {
                if (!map.containsKey(value.toString())){
                    map.put(value.toString(),1);
                }else{
                    map.replace(value.toString(),map.get(value.toString())+1);
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