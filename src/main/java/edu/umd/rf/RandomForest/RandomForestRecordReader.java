package edu.umd.rf.RandomForest;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomForestRecordReader extends RecordReader<LongWritable, LongWritable> {
	
	private int start;
	private int end;
	private int now;
	
	private LongWritable key;
	private LongWritable value;

	public RandomForestRecordReader(RandomForestInputSplit split) {
		System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.err.println("!!!!!!!!!!! create Record Reader         !!!!!!!");
		if (split == null){
			System.err.println("split is null!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		}
		System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		
		this.start = split.getStart();
		this.end = split.getEnd();
		this.now = split.getStart();		
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public LongWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (this.now == this.end){
			return 0;
		}else{
			return Math.min(1, (this.now - this.start)/ (float)(this.end - this.start) );
		}
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RandomForestInputSplit split = (RandomForestInputSplit) inputSplit;
		this.start = split.getStart();
		this.end = split.getEnd();
		this.now = split.getStart();
		
		this.key = new LongWritable(this.now);
		this.value = new LongWritable(this.now);		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.err.println("!!!!!!!!!!! trying to get next key value !!!!!!!");
		System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		if (this.now < this.end){
			if (key == null) System.err.println("============== key is null");
			else System.err.println("============== key " + key.toString());
			
			key.set(this.now);
			value.set(this.now);
			this.now++;
			return true;
		}
		return false;
	}

}
