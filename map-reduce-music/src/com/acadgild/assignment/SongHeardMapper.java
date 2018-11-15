package com.acadgild.assignment;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongHeardMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
		String[] lineArray = value.toString().split("\\|");
		Text trackId = new Text();
		IntWritable songHeard = new IntWritable();
		trackId.set(lineArray[1]);
		songHeard.set(Integer.parseInt(lineArray[4]));
		context.write(trackId, songHeard);
	}
}

