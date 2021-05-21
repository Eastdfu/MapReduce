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

public class CopyOfWordCount extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		Job job1 = new Job(getConf(), "WordCount");
		job1.setJarByClass(CopyOfWordCount.class);
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Map1.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Map2.class);
		job1.setReducerClass(Reduce.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		Job job2 = new Job(getConf(), "WordCount");
		job2.setJarByClass(CopyOfWordCount.class);
        job2.setJarByClass(WordCount.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        job2.setMapperClass(Map3.class);
        job2.setReducerClass(Reduce.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		if (job1.waitForCompletion(true)) {
			return job2.waitForCompletion(true) ? 0 : 1;
		}
		return 1;
	}
	
	public static void main(String[] args) throws Exception {
		int rel = ToolRunner.run(new Configuration(), new CopyOfWordCount(), args);
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
            		word.set("1:" + temp);
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
            		word.set("2:" + temp);
            		context.write(word, ONE);
            	}
            }
        }
    }
    
    public static class Map3 extends Mapper<LongWritable, Text, Text, IntWritable> {
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
            		if (token.charAt(0) == '1') {
            			word.set("1 : length " + temp.length());
            		}
            		else if (token.charAt(0) == '2') {
            			word.set("2 : length " + temp.length());
            		}
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
