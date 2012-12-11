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
	            		if (s.equals("normal"))
	            			label = 0;
	            		if (s.equals("smurf"))
	            			label = 1;
	            	}
	            }
	        	Instance instance = new Instance(label, Instance.toDoubleArray(a));
	        	System.out.println("instance num feature = " +instance.numFeatures() + "  |   label = " + label);
	        	writer.append(new IntWritable(k++), instance);
	        }
			writer.close();
		}
	}
	
	public void run(String args[]) throws Exception{
		/*
		ArrayList<ValueLabelPair> valueList = new ArrayList<ValueLabelPair>();
		valueList.add(new ValueLabelPair(3, 1));
		valueList.add(new ValueLabelPair(1, 0));
		valueList.add(new ValueLabelPair(2, 0));
		valueList.add(new ValueLabelPair(1, 1));
		valueList.add(new ValueLabelPair(3, 1));
		Collections.sort(valueList, new Comparator<ValueLabelPair>(){
			public int compare(ValueLabelPair p1, ValueLabelPair p2){
				return Double.compare(p1.getValue(), p2.getValue());
			}
		});
		for (int i=0; i<valueList.size(); i++) 
			System.out.printf("(%.3f,%d) ", valueList.get(i).getValue(), valueList.get(i).getLabel());
		System.out.println();
		*/
		
		Configuration conf = new Configuration();
		prepareData(conf, args[0], new Path("data"));
		RandomForest rf = new RandomForest(1, 1);
		rf.train("data");
		
		// predict training data
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path("data"), conf);
		Instance instance = new Instance();
		IntWritable key = new IntWritable();
		int correct = 0, total = 0;
		while (reader.next(key, instance)){
			int pred = rf.predict(instance);
			correct += (pred == instance.getLabel()) ? 1 : 0;			
			total++;
		}
		reader.close();
		System.out.printf("training accuracy = %d/%d\n" , correct, total);
	}
	
	public static void main(String args[]) throws Exception{
		(new Driver()).run(args);
	}

}
