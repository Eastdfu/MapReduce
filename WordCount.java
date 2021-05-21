package edu.stanford.cs246.wordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "WordCount");
		job.setJarByClass(WordCount.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Map2.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int rel = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(rel);
	}
    
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            for (String token: value.toString().split("\\s+")) {
            	String temp = "";
            	for (int i = 0; i < token.length(); i++) {
            		if (token.charAt(i) >= 'a' && token.charAt(i) <= 'z') {
            			temp += token.charAt(i);
            		}
            		else if (token.charAt(i) >= 'A' && token.charAt(i) <= 'Z') {
            			temp += Character.toLowerCase(token.charAt(i));
            		}
            	}
            	if (temp.length() == 35) {
            		System.out.print(token);
            	}
            	if (temp.length() > 0) {
            		word.set("1 : length " + temp.length());
            		context.write(word, ONE);
            	}
            }
        }
    }
    
    public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            for (String token: value.toString().split("\\s+")) {
            	String temp = "";
            	for (int i = 0; i < token.length(); i++) {
            		if (token.charAt(i) >= 'a' && token.charAt(i) <= 'z') {
            			temp += token.charAt(i);
            		}
            		else if (token.charAt(i) >= 'A' && token.charAt(i) <= 'Z') {
            			temp += Character.toLowerCase(token.charAt(i));
            		}
            	}
            	if (temp.length() == 35) {
            		System.out.print(token);
            	}
            	if (temp.length() > 0) {
            		word.set("2 : length " + temp.length());
            		context.write(word, ONE);
            	}
            }
        }
    }
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
