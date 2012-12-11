package edu.umd.rf.BreathFirstRandomForest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.rf.BreathFirstRandomForest.Models.SplitStatistics;

public class FindBestSplitJob {
	
	public static void run(Configuration conf, Path inputPath, Path outputPath, Tree tree) throws Exception{
		 
		Job job = new Job(conf, "determine best split for each leaf");
		job.setJarByClass(FindBestSplitJob.class);
		job.setMapperClass(Mapper.class);		
		job.setReducerClass(BestSplitReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SplitStatistics.class);
		
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
	}
	
	// maybe: can generate a list of nodes containing result?
	
	public static class BestSplitReducer extends Reducer <IntWritable, SplitStatistics, IntWritable, SplitStatistics > {
		
		// each node best split features
		@Override
		protected void reduce(IntWritable key, Iterable<SplitStatistics> values, Context context) throws IOException, InterruptedException {
			SplitStatistics s = null;
			for (SplitStatistics value : values){
				if (s == null || s.getSplitScore() > value.getSplitScore()){
					s = value;
				}
			}			
			context.write(key, s);			
		}
	}	
}
