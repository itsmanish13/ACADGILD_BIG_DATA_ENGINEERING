package com.acadgild.assignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Music {
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapreduce.task.io.sort.mb","128");
		Job job = new Job(conf, "Music");
		job.setJarByClass(Music.class);
		
		if(args[0].equalsIgnoreCase("uniqueListener")) {
			job.setMapOutputKeyClass(String.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(String.class);
			job.setOutputValueClass(NullWritable.class);
			job.setMapperClass(UniqueListenerMapper.class);
			job.setReducerClass(UniqueListenerReducer.class);
		}else if(args[0].equalsIgnoreCase("songHeared")) {
			job.setMapOutputKeyClass(String.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(String.class);
			job.setOutputValueClass(IntWritable.class);
			job.setMapperClass(SongHeardMapper.class);
			job.setReducerClass(SongHeardReducer.class);
		}else if(args[0].equalsIgnoreCase("songShared")) {
			job.setMapOutputKeyClass(String.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(String.class);
			job.setOutputValueClass(IntWritable.class);
			job.setMapperClass(SongSharedMapper.class);
			job.setReducerClass(SongSharedReducer.class);
		}else {
			System.out.println("Please use any of below options: \n 1.uniqueListener \n 2.songHeared \n 3.songShared");
			System.exit(0);
		}
			
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1])); 
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
		
		job.waitForCompletion(true);
		
	}
}
