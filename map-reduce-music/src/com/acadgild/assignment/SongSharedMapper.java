package com.acadgild.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongSharedMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
		String[] lineArray = value.toString().split("\\|");
		Text trackId = new Text();
		IntWritable songShared = new IntWritable();
		trackId.set(lineArray[1]);
		songShared.set(Integer.parseInt(lineArray[2]));
		context.write(trackId, songShared);
	}
}


