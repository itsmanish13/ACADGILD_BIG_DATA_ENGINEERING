package com.acadgild.assignment;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueListenerReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<NullWritable> value,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.reduce(key, value, context);
		context.write(key, NullWritable.get());
	}
}
