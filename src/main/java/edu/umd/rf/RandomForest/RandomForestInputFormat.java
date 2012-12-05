package edu.umd.rf.RandomForest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomForestInputFormat extends InputFormat<LongWritable, LongWritable>{

	@Override
	public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!! create recordreader !!!!!!!");
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

		return new RandomForestRecordReader( (RandomForestInputSplit)split ); 
	}

	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
			InterruptedException {
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!! trying to get split !!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		//int numSplits = jobContext.getConfiguration().getInt("numSplits", 1);
		//int numTrees  = jobContext.getConfiguration().getInt("numTrees", 1);
		int numSplits = Driver.NUMSPLIT;
		int numTrees = Driver.NUMTREE;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int size = (numTrees + numSplits - 1) / numSplits;
		for (int i=0; i < numSplits; i++){
			splits.add(new RandomForestInputSplit(i * size, Math.min(numTrees, (i+1)*size)) );
			System.err.println("range generated: " + (i * size) + " " + Math.min(numTrees, (i+1)*size));
		}
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!! finish generating split !!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

		return splits;
	}

}
