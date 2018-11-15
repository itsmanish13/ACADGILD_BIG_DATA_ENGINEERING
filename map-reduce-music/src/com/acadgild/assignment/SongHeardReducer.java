package com.acadgild.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongHeardReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(key, value, context);
		IntWritable total = new IntWritable();
		Integer sum = new Integer(0);
		for (IntWritable val : value) {
			sum = sum + val.get();
		}
		total.set(sum);
		context.write(key, total);
	}
}
