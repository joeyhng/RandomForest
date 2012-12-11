package edu.umd.rf.BreathFirstRandomForest.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Instance implements Writable {
	
	private int label;
	private double[] data;
	
	public Instance(){}
	
	public Instance(int label, double[] data){
		this.label = label;
		this.data = data;						
	}
	
	public int numFeatures(){
		return data.length;
	}
	
	public double get(int featureID){
		return data[featureID];		
	}
	
	public int getLabel() {
		return label;
	}

	public void readFields(DataInput input) throws IOException {
		this.label = input.readInt();
		int numFeatures = input.readInt();
		this.data = new double[numFeatures];
		for (int i=0; i<data.length; i++)
			data[i] = input.readDouble();
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(label);
		output.writeInt(data.length);
		for (int i=0; i<data.length; i++)
			output.writeDouble(data[i]);
	}
	
	public static double[] toDoubleArray(ArrayList<Double> a){
		double[] b = new double[a.size()];
		for (int i=0; i<b.length; i++) b[i] = a.get(i);
		return b;
	}

}
