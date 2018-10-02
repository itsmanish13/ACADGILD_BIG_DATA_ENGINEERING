package org.acadgild.hadoop.map_reduce_prog;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalSalesbyStateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
IntWritable totalSales;
	
	@Override
	public void setup(Context context) {
		totalSales = new IntWritable();
	}
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		Integer sum = new Integer(0);
		for (IntWritable value : values) {
			sum = sum + value.get();
		}
		totalSales.set(sum);
		context.write(key, totalSales);
	}
}
