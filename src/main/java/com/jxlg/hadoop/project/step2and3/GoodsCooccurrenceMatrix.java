package com.jxlg.hadoop.project.step2and3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @program: grms
 * @package: com.jxlg.hadoop.project.step3
 * @filename: GoodsCooccurrenceMatrix.java
 * @create: 2019/10/12 12:41
 * @author: 29314
 * @description: .计算商品的共现关系与共现次数(共现矩阵)
 **/

public class GoodsCooccurrenceMatrix extends Configured implements Tool {
    public static class GoodsCooccurrenceMatrixMapper extends Mapper<Text, Text, Text, Text> {
        private Text k2 = new Text();
        private Text v2 = new Text();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("[,]");
            for(int i = 0;i<strs.length;i++){
                for(int j = 0;j<strs.length;j++ ){
                    if(strs[i].equals(strs[j])){
                        continue;
                    }
                    this.k2.set(strs[i]);
                    this.v2.set(strs[j]);
                    context.write(this.k2,this.v2);
                }
            }
        }
    }

    public static class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text, Text, Text> {
        private Text k3 = new Text();
        private Text v3 = new Text();
        Map<String,Integer> map = new TreeMap<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            this.k3.set(key.toString());
            for(Text v2:values){
                String goods = v2.toString();
                map.put(goods,map.containsKey(goods)?map.get(goods)+1:1);
            }
            StringBuilder sb = new StringBuilder();
            map.forEach((k,v)->{
                sb.append(k).append(":").append(v).append(",");
            });
            this.v3.set(sb.substring(0,sb.length()-1));
            map.clear();
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //数据来源于step1
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "计算商品的共现关系与共现次数(共现矩阵)");
        job.setJarByClass(this.getClass());

        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job,in);

        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(),args));
    }
}
