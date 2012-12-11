package edu.umd.rf.BreathFirstRandomForest;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umd.rf.BreathFirstRandomForest.Models.Instance;
import edu.umd.rf.BreathFirstRandomForest.Models.NodeFeaturePair;
import edu.umd.rf.BreathFirstRandomForest.Models.SplitStatistics;
import edu.umd.rf.BreathFirstRandomForest.Models.ValueLabelPair;

public class Tree implements Writable {
	
	private int maxDepth;
	private Node root;
	
	private ArrayList<Node> nodes; 
	
	public Tree(){
		this.root = new Node();
		this.root.setID(0);
		fillNodeList();
	}
	
	public Tree(int maxDepth){
		this();
		this.maxDepth = maxDepth;		
	}

	private void fillNodeList(){
		this.nodes = new ArrayList<Node>(size());
		for (int i = 0; i < size(); i++) nodes.add(null);
		this.root.fillNodeList(this.nodes);
	}

	
	public Node predictNode(Instance instance){
		return root.predict(instance);
	}
	
	public int predict(Instance instance){
		return predictNode(instance).getMajorityLabel();
	}
	
	private void checkFile(FileSystem fs, Configuration conf) throws IOException{
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("leafscore/part-r-00000"), conf);
		/*
		NodeFeaturePair k = new NodeFeaturePair();
		ValueLabelPair v = new ValueLabelPair();
		while (reader.next(k, v)) {
			System.out.println(k.getNodeID()+ " " + k.getFeatureID() + " -> " + v.getValue());
		}
		*/			
		IntWritable key = new IntWritable();
		SplitStatistics split = new SplitStatistics();
		while (reader.next(key, split)) {
			System.out.println(key.get() + " " + split.getFeatureID() + " " + split.getSplitValue() + " " + split.getSplitScore());
		}			
		reader.close();
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
	}
	
	public void train() throws Exception{
		// 1. pre-process data to SequenceFile 
		// 2. prepare root node
		for (int nowDepth = 0; nowDepth < maxDepth; nowDepth++){
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.out.println("!!!!!!!!!!    depth "+ nowDepth +"  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			// gather feature statistics for leaves
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path("data");
			Path outputPath = new Path("leafscore");
			DetermineLeafJob.run(conf, inputPath, outputPath, this);
			
			checkFile(fs, conf);
			
			// get best split for leafs
			// next step: single Map : <(Node, feature, score)> -> <Node, best feature>			
			FindBestSplitJob.run(conf, outputPath, new Path("out"), this);

			// TODO: prune leaves?
			// expand leaves
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("out/part-r-00000"), conf);
			IntWritable nid = new IntWritable();
			SplitStatistics bestSplit = new SplitStatistics();
			while (reader.next(nid, bestSplit)) {
				int leafID = nid.get();
				System.out.println("leaf ID = " + leafID);
				Node node = nodes.get(leafID);
				node.setSplit(bestSplit, this.nodes);
				fillNodeList();
				System.out.println("node list size = " + this.nodes.size());
			}
			System.out.println("size = " + size());
			
			fs.delete(outputPath, true);
			fs.delete(new Path("out"), true);
		}
	}
	
	public int size(){
		return root.size();
	}

	public void readFields(DataInput input) throws IOException {
		root.readFields(input);
		this.maxDepth = input.readInt();
		this.nodes = new ArrayList<Node>(root.size());
		for (int i=0; i < size(); i++) 
			nodes.add(null);
		this.root.fillNodeList(this.nodes);
	}

	public void write(DataOutput output) throws IOException {
		root.write(output);
		output.writeInt(maxDepth);		
	}

}
