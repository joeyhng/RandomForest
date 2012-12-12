package edu.umd.rf.BreathFirstRandomForest;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import edu.umd.rf.BreathFirstRandomForest.Models.Instance;
import edu.umd.rf.BreathFirstRandomForest.Models.ValueLabelPair;

public class Driver {
	

	public void prepareData(Configuration conf, String inputFile, Path dataPath) throws IOException{
		try{
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), dataPath, conf);
			reader.close();
		} catch (IOException e){
			System.out.println("reading file");
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, dataPath, IntWritable.class, Instance.class);
			int k = 0;
	        File file = new File(inputFile);
	        Scanner fileScanner = new Scanner(file);
	        while (fileScanner.hasNextLine()){
	            String line = fileScanner.nextLine().replaceAll(",", " ");
	            Scanner sc = new Scanner(line);
	            
	            ArrayList<Double> a = new ArrayList<Double>();
	            int label = 0;
	            while (sc.hasNext()){
	            	String s = sc.next();
	            	try{
	            		a.add( Double.parseDouble(s) );
	            	} catch (NumberFormatException nfe){
	            		/*
	            		if (s.equals("normal"))	label = 0;
	            		if (s.equals("smurf")) label = 1;
	            		*/
	            		if (s.equals("g"))	label = 0;
	            		if (s.equals("b")) label = 1;
	            	}
	            }
	        	Instance instance = new Instance(label, Instance.toDoubleArray(a));
	        	System.out.println("instance num feature = " +instance.numFeatures() + "  |   label = " + label);
	        	writer.append(new IntWritable(k++), instance);
	        }
			writer.close();
		}
	}
	
	private void evaluation(RandomForest rf, Configuration conf, Path path) throws IOException{
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
		Instance instance = new Instance();
		IntWritable key = new IntWritable();
		int correct = 0, total = 0;
		int c0=0, c1=0;
		while (reader.next(key, instance)){
			int pred = rf.predict(instance);
			if (pred==0) c0++;
			if (pred==1) c1++;
			correct += (pred == instance.getLabel()) ? 1 : 0;			
			total++;
		}
		reader.close();
		System.out.println(c0 + " " + c1);
		System.out.printf("accuracy = %d/%d\n" , correct, total);

	}
	
	public void run(String args[]) throws Exception{

		String trainPath = "/classhomes/cs71407/rf/data/train_ionosphere.data";
		String testPath = "/classhomes/cs71407/rf/data/test_ionosphere.data";
		
		Configuration conf = new Configuration();
		prepareData(conf, trainPath, new Path("data"));
		RandomForest rf = new RandomForest(3, 20);
		rf.train("data");
		
		// predict training data
		prepareData(conf, testPath, new Path("testdata"));
		evaluation(rf, conf, new Path("data"));
		evaluation(rf, conf, new Path("testdata"));		
	}
	
	public static void main(String args[]) throws Exception{
		(new Driver()).run(args);
	}

}
