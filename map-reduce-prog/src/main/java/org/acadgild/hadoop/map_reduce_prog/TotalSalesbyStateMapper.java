package org.acadgild.hadoop.map_reduce_prog;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalSalesbyStateMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		Text brand = new Text();
		IntWritable num = new IntWritable();
		
		String[] lineArray = value.toString().split("\\|");
		
		brand.set(lineArray[0].toUpperCase()+"-"+lineArray[3].toUpperCase());
		num.set(1);
		
		context.write(brand, num);
	}

}
