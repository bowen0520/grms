package com.jxlg.hadoop.project.step5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.step5
 * @filename: MultiplyGoodsMatrixAndUserVector.java
 * @create: 2019/10/12 16:06
 * @author: 29314
 * @description: .商品共现矩阵乘以用户购买向量，形成临时的推荐结果
 **/

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {
    public static class FirstMapper extends Mapper<Text, Text, KeyValueData, Text> {
        private KeyValueData k2 = new KeyValueData();
        private Text v2 = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            this.k2.setK(key.toString());
            this.k2.setV(1);
            this.v2.set(value.toString());
            System.out.println(1+"::::::"+key.toString()+"::::::::::"+value.toString());
            context.write(this.k2,this.v2);
        }
    }

    public static class SecondMapper extends Mapper<Text, Text, KeyValueData, Text> {
        private KeyValueData k2 = new KeyValueData();
        private Text v2 = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            this.k2.setK(key.toString());
            this.k2.setV(2);
            this.v2.set(value.toString());
            System.out.println(2+"::::::"+key.toString()+"::::::::::"+value.toString());
            context.write(this.k2,this.v2);
        }
    }

    public static class MyGroupingComparator extends WritableComparator{
        public MyGroupingComparator() {
            super(KeyValueData.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyValueData ka = (KeyValueData) a;
            KeyValueData kb = (KeyValueData) b;
            return ka.getK().compareTo(kb.getK());
        }
    }

    public static class MySortComparator extends WritableComparator{
        public MySortComparator() {
            super(KeyValueData.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            KeyValueData ka = (KeyValueData) a;
            KeyValueData kb = (KeyValueData) b;
            int n=ka.getK().compareTo(kb.getK());
            return n==0?ka.getV()-kb.getV():n;
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<KeyValueData, Text, Text, IntWritable> {
        private Text k3 = new Text();
        private IntWritable v3 = new IntWritable();
        @Override
        protected void reduce(KeyValueData key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String users = iterator.next().toString();
            String goods = iterator.next().toString();

            System.out.println(key.getK()+"::::::"+users+"::::::"+goods);

            String[] us = users.split(",");
            String[] gs = goods.split(",");

            for(String u:us){
                for(String g:gs){
                    String[] ss = g.split(":");
                    this.k3.set(u+","+ss[0]);
                    this.v3.set(Integer.parseInt(ss[1]));
                    context.write(this.k3,this.v3);
                }
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //数据来源于step4 和 step3
        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "商品共现矩阵乘以用户购买向量");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(
                job,in1, SequenceFileInputFormat.class,FirstMapper.class);
        MultipleInputs.addInputPath(
                job,in2,SequenceFileInputFormat.class,SecondMapper.class);
        //设置mapper的输出key，value的数据类型
        job.setMapOutputKeyClass(KeyValueData.class);
        job.setMapOutputValueClass(Text.class);

        job.setGroupingComparatorClass(MyGroupingComparator.class);
        job.setSortComparatorClass(MySortComparator.class);

        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(),args));
    }
}
