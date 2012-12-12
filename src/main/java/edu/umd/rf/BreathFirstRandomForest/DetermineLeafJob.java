package edu.umd.rf.BreathFirstRandomForest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.rf.BreathFirstRandomForest.Models.Instance;
import edu.umd.rf.BreathFirstRandomForest.Models.NodeFeaturePair;
import edu.umd.rf.BreathFirstRandomForest.Models.SplitStatistics;
import edu.umd.rf.BreathFirstRandomForest.Models.ValueLabelPair;


public class DetermineLeafJob {
	
	private static final int NUMFEATURES = 32;
	/*
	 *  determine leaf/feature and gather statistics
	 *  give sufficient statistics for calculate splitting score for each node/feature pair
	 */
	
	public static void run(Configuration conf, Path inputPath, Path outputPath, Tree tree) throws Exception{
		
		FileSystem fs = FileSystem.get(conf);
		DataOutputStream dos = new DataOutputStream(fs.create(new Path("tree")));
		tree.write(dos);
		dos.close();
		
		ArrayList<ArrayList<Integer>> usedFeatures = new ArrayList<ArrayList<Integer>>();
		for (int i=0; i<tree.size(); i++){
			ArrayList<Integer> f = new ArrayList<Integer>();
			Random rand = new Random();
			for (int j=0; j<8; j++)
				f.add(rand.nextInt(NUMFEATURES));
			usedFeatures.add(f);			
		}
		ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path("featureList")));
		oos.writeObject(usedFeatures);
		oos.close();
		
		ObjectInputStream ois = new ObjectInputStream(fs.open(new Path("featureList")));
		usedFeatures = (ArrayList<ArrayList<Integer>>)ois.readObject();
		ois.close();	

		
		System.out.println("before trying read tree");
		DataInputStream dis = new DataInputStream(FileSystem.get(conf).open(new Path("tree")));
		tree = new Tree();
		tree.readFields(dis);
		dis.close();
		System.out.println("after trying read tree");
		
		
		System.out.println("tree size = " + tree.size());

		Job job = new Job(conf, "determine leaf node for each data instance");
		job.setJarByClass(DetermineLeafJob.class);
		
		job.setMapperClass(DetermineLeafMapper.class);
		job.setMapOutputKeyClass(NodeFeaturePair.class);
		job.setMapOutputValueClass(ValueLabelPair.class);
		
		/*
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(NodeFeaturePair.class);
		job.setOutputValueClass(ValueLabelPair.class);
		*/		
		
		job.setReducerClass(DetermineLeafReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SplitStatistics.class);
		
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
		fs.delete(new Path("tree"), true);
				
		/*
			DistributedCache.addCacheFile(new URI("data#localdata"), conf);
			DistributedCache.createSymlink(conf);
		 */
	}
	
	
	public static class DetermineLeafMapper extends Mapper<IntWritable, Instance, NodeFeaturePair, ValueLabelPair> {

		static private Tree tree = null;
		static private ArrayList<ArrayList<Integer> > usedFeatures = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (tree == null) {
				DataInputStream dis = new DataInputStream(FileSystem.get(context.getConfiguration()).open(new Path("tree")));
				tree = new Tree();
				tree.readFields(dis);
				dis.close();
			}
			if (usedFeatures == null){
				try {
					ObjectInputStream ois = new ObjectInputStream(FileSystem.get(context.getConfiguration()).open(new Path("featureList")));
					usedFeatures = (ArrayList<ArrayList<Integer>>)ois.readObject();
					ois.close();	
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void map(IntWritable key, Instance instance, Context context) throws IOException, InterruptedException {
			// predict which Node 
			int n = tree.predictNode(instance).getID(); // must be currently a leaf
			ArrayList<Integer> featList = usedFeatures.get(n);
			for (int i=0; i<featList.size(); i++){
				int f = featList.get(i).intValue();
				context.write(new NodeFeaturePair(n, f), new ValueLabelPair(instance.get(f), instance.getLabel()));
			}
		}
	}
	
	public static class DetermineLeafReducer extends Reducer <NodeFeaturePair, ValueLabelPair, IntWritable, SplitStatistics > {
		
		private double gini(double x,double y){
			double p = ((double)x) / (x + y);
			return p * (1 - p);
		}
		
		private double computeScore(int[] left, int[] total){
			int[] right = {total[0] - left[0], total[1] - left[1]};
			return ((double)left[0]+left[1]) / (total[0]+total[1]) * gini(left[0], left[1]) +  
				   ((double)right[0]+right[1]) / (total[0]+total[1]) * gini(right[0], right[1]);
		}

		// each node best split features
		@Override
		protected void reduce(NodeFeaturePair key, Iterable<ValueLabelPair> values, Context context) throws IOException, InterruptedException {
			
			int[] total = {0, 0};
			ArrayList<ValueLabelPair> valueList = new ArrayList<ValueLabelPair>();
			for (ValueLabelPair value: values) {
				total[ (value.getLabel() == 1) ? 1 : 0] += 1;
				valueList.add(new ValueLabelPair(value.getValue(), value.getLabel()));
			}
			
			if (total[0]==0 || total[1]==0) // perfect training classification
				return;
	        			
			Collections.sort(valueList, new Comparator<ValueLabelPair>(){
				public int compare(ValueLabelPair p1, ValueLabelPair p2){
					return Double.compare(p1.getValue(), p2.getValue());
				}
			});
			
			double splitScore = -1;
			double splitValue = 0;
			int leftLabel = -1, rightLabel = -1;
			int[] current = {0, 0};
			for (int i = 0; i < valueList.size(); i++){
				ValueLabelPair value = valueList.get(i);
				if (i > 0 && Math.abs(value.getValue() - valueList.get(i-1).getValue()) > 1e-8){
					double score = computeScore(current, total);
					if (splitScore == -1 || score < splitScore){
						splitValue = value.getValue();
						splitScore = score;
						leftLabel = (current[0] > current[1]) ? 0 : 1; 
						rightLabel = (total[0] - current[0] > total[1] - current[1]) ? 0 : 1;
								
					}				
				}
				current[ (value.getLabel() == 1) ? 1 : 0] += 1;
			}
			
			if (splitScore != -1){ // if splitable by that feature (not all feature has same value)
				context.write(new IntWritable(key.getNodeID()), new SplitStatistics(key.getFeatureID(), splitValue, splitScore, leftLabel, rightLabel));
			}
		}
	}
}
