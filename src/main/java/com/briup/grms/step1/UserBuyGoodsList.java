package com.briup.bigdata.project.grms.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserBuyGoodsList {

    public static class UserBuyGoodsListMap extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }

    public static class UserBuyGoodsListReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str = "";
            for (Text value : values) {
                str = str+","+value;
            }
            String substring = str.substring(1, str.length());
            context.write(key,new Text(substring));
        }
    }
}