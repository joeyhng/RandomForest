package edu.umd.rf.RandomForest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class RandomForestInputSplit extends InputSplit implements Writable{
	
	private int start;
	private int end;
	
	public RandomForestInputSplit(){}

	public RandomForestInputSplit(int start, int end) {
		System.err.println("!!!!!!!!!!! create randomforestinputsplit !!!!!!!");

		this.start = start;
		this.end = end;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return (this.end - this.start) * 8;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
	}
	
	public int getStart(){
		return start;
	}
	
	public int getEnd(){
		return end;
	}

	public void readFields(DataInput in) throws IOException {
		this.start = in.readInt();
		this.end = in.readInt();		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(start);
		out.writeInt(end);	
	}

}
