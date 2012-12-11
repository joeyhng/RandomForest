package edu.umd.rf.BreathFirstRandomForest.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SplitStatistics implements Writable {
	

	private int featureID;
	private double splitValue;
	private double splitScore;

	private int leftLabel;
	private int rightLabel;
	
	public SplitStatistics(){}

	public SplitStatistics(int featureID, double splitValue, double splitScore, int leftLabel, int rightLabel) {
		this.featureID = featureID;
		this.splitValue = splitValue;
		this.splitScore = splitScore;
		this.leftLabel = leftLabel;
		this.rightLabel = rightLabel;
	}

	public void readFields(DataInput input) throws IOException {
		this.featureID = input.readInt();
		this.splitValue = input.readDouble();
		this.splitScore = input.readDouble();
		this.leftLabel = input.readInt();
		this.rightLabel = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(this.featureID);
		output.writeDouble(this.splitValue);
		output.writeDouble(this.splitScore);
		output.writeInt(this.leftLabel);
		output.writeInt(this.rightLabel);
	}

	public int getFeatureID() {
		return featureID;
	}

	public double getSplitValue() {
		return splitValue;
	}

	public double getSplitScore() {
		return splitScore;
	}
	
	public int getLeftLabel() {
		return leftLabel;
	}

	public int getRightLabel() {
		return rightLabel;
	}
}
