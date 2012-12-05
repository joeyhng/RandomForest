package edu.umd.rf.RandomForest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class Tree implements Writable {

	private static int subsetSize = 30000;
	private int maxDepth;
	private Node root;
	
	public Tree(){
		this.maxDepth = 10;
	}

	public Tree(int maxDepth) {
		this.maxDepth = maxDepth;
		this.root = null;
	}
	
	public String toString(){
		return "tree with maxDepth="+maxDepth+" root node is "+((root==null)?"":"not ")+ "null";
	}

	public void train(Data data) {
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!!!!!!!!!!!! training random tree !!!!!!!!!!!!!!!!!!!!!!");
		System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		this.root = train_helper(data, 0);
	}

	private Node train_helper(Data data, int depth) {

		if (depth == this.maxDepth || data.monotone()) {
			return new Leaf(data.getMajority());
		}

		int[] selectedFeatures = data.featureRandomSubset(subsetSize);
		double maxScore = -1e30;
		int bestFeat = -1;
		for (int f = 0; f < selectedFeatures.length; f++) {
			int feat = selectedFeatures[f];
			double score = data.computeScore(feat);
			if (bestFeat == -1 || maxScore > score) {
				bestFeat = feat;
				maxScore = score;
			}
		}
		// System.out.println("max score = " + maxScore);

		if (maxScore <= 0) {
			return new Leaf(data.getMajority());
		}

		DataPair dataSplit = data.split(bestFeat);
		if (dataSplit == null) {
			return new Leaf(data.getMajority());
		}
		data = null;
		Node leftNode = train_helper(dataSplit.left(), depth + 1);
		Node rightNode = train_helper(dataSplit.right(), depth + 1);
		Node res = new InnerNode(bestFeat, leftNode, rightNode);
		if (leftNode.getClass().getName().equals("InnerNode"))
			System.out.println(leftNode.getClass().getName() + " "
					+ rightNode.getClass().getName());
		return res;
	}

	public int predict(ArrayList<Integer> data) {
		return root.predict(data);
	}

	public Node readTree(DataInput in) throws IOException {
		boolean isLeaf = in.readBoolean();
		if (isLeaf) {
			int majority = in.readInt();
			return new Leaf(majority);
		} else {
			int feat = in.readInt();
			Node left = readTree(in);
			Node right = readTree(in);
			return new InnerNode(feat, left, right);
		}
	}

	public void readFields(DataInput in) throws IOException {
		this.maxDepth = in.readInt();
		try{
			this.root = readTree(in);
		} catch (EOFException e) {
			this.root = null;
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.maxDepth);
		if (root != null)
			root.writeTree(out);
	}

}
