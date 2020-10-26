package com.briup.bigdata.project.grms.Test8;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SaveRecommendResultToDB {

    public static class SaveRecommendResultToDBMap extends Mapper<LongWritable, Text, GrmsBean, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            GrmsBean gb = new GrmsBean(split[0], split[1], Integer.parseInt(split[2]));
            context.write(gb, new IntWritable(1));
        }
    }

    public static class SaveRecommendResultToDBReduce extends Reducer<GrmsBean, IntWritable, GrmsBean, NullWritable> {
        @Override
        protected void reduce(GrmsBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key, NullWritable.get());
            }

        }
    }

}