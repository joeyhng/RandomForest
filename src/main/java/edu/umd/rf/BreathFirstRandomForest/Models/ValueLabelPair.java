package edu.umd.rf.BreathFirstRandomForest.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ValueLabelPair implements WritableComparable {
		
	private double value;
	private int label;
	
	public ValueLabelPair(){}
	
	public ValueLabelPair(double value, int label){
		this.value = value;
		this.label = label;		
	}
	
	public double getValue(){
		return value;
	}
	
	public void readFields(DataInput input) throws IOException {
		this.value = input.readDouble();
		this.label = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeDouble(this.value);
		output.writeInt(this.label);
	}

	public int getLabel() {
		return label;
	}

	public int compareTo(Object o) {
		ValueLabelPair p = (ValueLabelPair)o;
		if (value > p.value) return 1;
		if (value < p.value) return -1;
		if (label > p.label) return 1;
		if (label < p.label) return -1;
		return 0;	}

}
