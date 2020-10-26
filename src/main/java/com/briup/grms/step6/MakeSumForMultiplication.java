package com.briup.bigdata.project.grms.step6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MakeSumForMultiplication {

    public static class MakeSumForMultiplicationMap extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new Text(split[0]),new IntWritable(Integer.parseInt(split[1])));
        }
    }

    public static class MakeSumForMultiplicationCombiner extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static class MakeSumForMultiplicationReduce extends Reducer<Text, IntWritable,Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
}