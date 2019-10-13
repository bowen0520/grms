package com.jxlg.hadoop.project.step7;

import com.jxlg.hadoop.project.step5.KeyValueData;
import com.jxlg.hadoop.project.step5.MultiplyGoodsMatrixAndUserVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.step7
 * @filename: DuplicateDataForResult.java
 * @create: 2019/10/13 10:35
 * @author: 29314
 * @description: .数据去重，在推荐结果中去掉用户已购买的商品信息
 **/

public class DuplicateDataForResult extends Configured implements Tool {
    public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k2 = new Text();
        private IntWritable v2 = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[\t]");
            int n = Integer.parseInt(strs[2]);
            if(n>0){
                this.k2.set(strs[0]+","+strs[1]);
                this.v2.set(n);
                context.write(this.k2,this.v2);
            }
        }
    }

    public static class SecondMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Text k2 = new Text();
        private IntWritable v2 = new IntWritable();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            this.k2.set(key.toString());
            this.v2.set(value.get());
            context.write(this.k2,this.v2);
        }
    }

    public static class DuplicateDataForResultReducer extends Reducer<Text, IntWritable, UserGoodsData, NullWritable> {
        private UserGoodsData k3 = new UserGoodsData();
        private NullWritable v3 = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int an = iterator.next().get();
            if(iterator.hasNext()){
                return ;
            }
            String[] strings = key.toString().split(",");
            this.k3.setUserid(strings[0]);
            this.k3.setGoodsid(strings[1]);
            this.k3.setLevel(an);
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //数据来源于原始数据 和 step6
        Path in1 = new Path(conf.get("in1"));
        Path in2 = new Path(conf.get("in2"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "在推荐结果中去掉用户已购买的商品信息,并入库");
        job.setJarByClass(this.getClass());

        MultipleInputs.addInputPath(
                job,in1, TextInputFormat.class, FirstMapper.class);
        MultipleInputs.addInputPath(
                job,in2,SequenceFileInputFormat.class, SecondMapper.class);
        //设置mapper的输出key，value的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(UserGoodsData.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        //配置数据库连接信息
        DBConfiguration.configureDB(
                job.getConfiguration(),
                "com.mysql.cj.jdbc.Driver","jdbc:mysql://192.168.112.121:8017/db_test",
                "root","root"
        );
        //配置数据输出信息
        DBOutputFormat.setOutput(job,"tbl_ugr","userid","goodsid","level");

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DuplicateDataForResult(),args));
    }
}
