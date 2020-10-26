package com.briup.bigdata.project.grms.step5_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job5 = Job.getInstance(conf, "");
        job5.setJarByClass(this.getClass());
        MultipleInputs.addInputPath(job5, new Path("/grms/step4/"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job5, new Path("/grms/step3/"), TextInputFormat.class, MultiplyGoodsMatrixAndUserVectorSecondMapper.class);
        job5.setReducerClass(MultiplyGoodsMatrixAndUserVectorReduce.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        job5.setPartitionerClass(MyPartition.class);
        job5.setGroupingComparatorClass(MyGroup.class);
        TextOutputFormat.setOutputPath(job5, new Path("/grms/step5/"));
        job5.waitForCompletion(true);
        return 0;
    }


    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<LongWritable, Text, MyKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new MyKey(split[0],0),new Text("a" + split[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<LongWritable, Text, MyKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("[\t]");
            context.write(new MyKey(split[0],1), new Text("b" + split[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorReduce extends Reducer<MyKey, Text, Text, Text> {
        @Override
        protected void reduce(MyKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            //两个value
            String s1 = iter.next().toString();
            String s2 = iter.next().toString();
            String[] split1 = s1.split("[,]");
            String[] split2 = s2.split("[,]");
            for (String ss1 : split1) {
                for (String ss2 : split2) {
                    String[] split11 = split1[1].split("[:]");
                    String[] split12 = split1[2].split("[:]");
                }
            }

        }
    }
}