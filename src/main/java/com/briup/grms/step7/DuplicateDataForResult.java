package com.briup.bigdata.project.grms.step7;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DuplicateDataForResult{

    public static class DuplicateDataForResultFirstMapper extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]+","+split[1]),new Text("a"+split[0]+","+split[1]));
        }
    }
    public static class DuplicateDataForResultSecondMapper extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]),new Text("b"+split[1]));
        }
    }

    public static class DuplicateDataForResultReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String num = null;
            String s = null;
            List<String> list = new ArrayList<>();
            for (Text value : values) {
                if (value.toString().startsWith("a")) {
                    s = value.toString().substring(1);
                    list.add(s);
                }else if (value.toString().startsWith("b")){
                    num = value.toString().substring(1);
                }
            }
            String[] split = key.toString().split("[,]");
            if (!list.contains(key.toString()))
                context.write(new Text(split[0]+"\t"+split[1]),new Text(num));
        }
    }
}