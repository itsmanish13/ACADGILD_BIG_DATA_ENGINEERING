package com.acadgild.assignment;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueListenerMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
		String[] lineArray = value.toString().split("\\|");
		Text userId = new Text();
		userId.set(lineArray[0]);
		context.write(userId, NullWritable.get());
	}
}
