package edu.umd.rf.BreathFirstRandomForest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

import edu.umd.rf.BreathFirstRandomForest.Models.Instance;
import edu.umd.rf.BreathFirstRandomForest.Models.SplitStatistics;

public class Node implements Writable {
	
	private int id;
	private int featureID;
	private double splitValue;
	
	private int majorityLabel; 

	private Node left;
	private Node right;
	
	public Node(){
		left = right = null;
		splitValue = majorityLabel = featureID = 0;
	}
	
	public Node(int id, int majorityLabel){
		this();
		this.id = id;
		this.majorityLabel = majorityLabel;
	}
	
	public Node predict(Instance instance){
		if (left == null && right == null)
			return this;
		if (instance.get(featureID) < splitValue){
			return left.predict(instance);
		} else {
			return right.predict(instance);
		}
	}

	public int getMajorityLabel() {
		return majorityLabel;
	}
	
	public void setMajorityLabel(int majorityLabel) {
		this.majorityLabel = majorityLabel;
	}
	
	public int getID(){
		return id;
	}
	
	public void setID(int id){
		this.id = id;
	}
	
	public int size(){
		if (left == null)
			return 1;
		return left.size() + right.size() + 1;
	}
		
	public void readFields(DataInput input) throws IOException {
		this.id = input.readInt();
		boolean isLeaf = input.readBoolean();
		if (!isLeaf){
			left = new Node();
			right = new Node();
			left.readFields(input);
			right.readFields(input);
		} else{
			left = right = null;
		}
		featureID = input.readInt();
		splitValue = input.readDouble();
		majorityLabel = input.readInt();
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(this.id);
		output.writeBoolean(left == null);
		if (left != null){
			assert left != null && right != null;
			left.write(output);
			right.write(output);
		}
		output.writeInt(featureID);
		output.writeDouble(splitValue);
		output.writeInt(majorityLabel);  
	}

	public void fillNodeList(ArrayList<Node> nodes) {
		nodes.set(id, this);
		if (left != null){
			left.fillNodeList(nodes);
			right.fillNodeList(nodes);
		}
	}

	public void setSplit(SplitStatistics bestSplit, ArrayList<Node> nodes) {
		this.featureID = bestSplit.getFeatureID();
		this.splitValue = bestSplit.getSplitValue();
		this.left = new Node(nodes.size(), bestSplit.getLeftLabel());
		this.right = new Node(nodes.size()+1, bestSplit.getRightLabel());
	}

}
