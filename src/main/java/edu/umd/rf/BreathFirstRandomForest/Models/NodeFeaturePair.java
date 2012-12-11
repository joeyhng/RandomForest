package edu.umd.rf.BreathFirstRandomForest.Models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.rf.BreathFirstRandomForest.Node;


public class NodeFeaturePair implements WritableComparable {
	
	private int nodeID;
	private int featureID;
	
	public NodeFeaturePair(){}

	public NodeFeaturePair(int nodeID, int f) {
		this.nodeID = nodeID;
		this.featureID = f;
	}

	public void readFields(DataInput input) throws IOException {
		this.nodeID = input.readInt();
		this.featureID = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(this.nodeID);
		output.writeInt(this.featureID);
	}

	public int getNodeID() {
		return nodeID;
	}
	
	public int getFeatureID(){
		return featureID;
	}

	public int compareTo(Object o) {
		NodeFeaturePair p = (NodeFeaturePair)o;
		if (nodeID > p.nodeID) return 1;
		if (nodeID < p.nodeID) return -1;
		if (featureID > p.featureID) return 1;
		if (featureID < p.featureID) return -1;		
		return 0;
	}

}
