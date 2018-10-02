package org.acadgild.hadoop.map_reduce_prog;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TelevisionSales {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AssignmentTask");
		job.setJarByClass(TelevisionSales.class);
		
		if (args[0].toString().equals("filter")) {
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setMapperClass(FilterMapper.class);
			job.setNumReduceTasks(0);
		} else if(args[0].toString().equals("totalSalesByBrand")) {
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setMapperClass(TotalSalesbyBrandMapper.class);
			job.setReducerClass(TotalSalesbyBrandReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
		} else if (args[0].toString().equals("totalSalesByState")) {
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setMapperClass(TotalSalesbyStateMapper.class);
			job.setReducerClass(TotalSalesbyStateReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
		}
	 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1])); 
		FileOutputFormat.setOutputPath(job,new Path(args[2]));

		/*
		Path out=new Path(args[1]);
		out.getFileSystem(conf).delete(out);
		*/
		
		job.waitForCompletion(true);
	}
}
