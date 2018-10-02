package org.acadgild.hadoop.map_reduce_prog;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		IntWritable val = new IntWritable();
		val.set(1);
		String[] lineArray = value.toString().split("\\|");
		
		if(!lineArray[0].equals("NA") && !lineArray[1].equals("NA")) {
			context.write(value, null);
		}
			
	}
}