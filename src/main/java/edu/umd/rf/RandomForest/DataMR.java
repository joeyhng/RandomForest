package edu.umd.rf.RandomForest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

public class DataMR extends Data {

	private static final long serialVersionUID = -273390413888004934L;

	static private BASE64Encoder encode = new BASE64Encoder();
	static private BASE64Decoder decode = new BASE64Decoder();

	public DataMR(TreeMap<Integer, Integer> labels, TreeMap<Integer, int[]> data) {
		super(labels, data);
	}

	@Override
	public DataPair split(int feat) {
		HashSet<Integer> index = new HashSet<Integer>();
		int[] examples = data.get(new Integer(feat));
		if (examples == null)
			return null;

		for (int i = 0; i < examples.length; i++) {
			index.add(examples[i]);
		}

		TreeMap<Integer, Integer> leftLabels = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Integer> rightLabels = new TreeMap<Integer, Integer>();
		for (Map.Entry<Integer, Integer> entry : labels.entrySet()) {
			if (index.contains(entry.getKey())) {
				leftLabels.put(entry.getKey(), entry.getValue());
			} else {
				rightLabels.put(entry.getKey(), entry.getValue());
			}
		}

		TreeMap<Integer, int[]> leftMap = new TreeMap<Integer, int[]>();
		TreeMap<Integer, int[]> rightMap = new TreeMap<Integer, int[]>();

		if (data.size() > 10) {
			computeSplitMR(index, leftMap, rightMap);
		} else {
			for (Map.Entry<Integer, int[]> entry : data.entrySet()) {
				ArrayList<Integer> leftList = new ArrayList<Integer>();
				ArrayList<Integer> rightList = new ArrayList<Integer>();

				examples = entry.getValue();
				for (int j = 0; j < examples.length; j++) {
					if (index.contains(examples[j])) {
						leftList.add(examples[j]);
					} else {
						rightList.add(examples[j]);
					}
				}

				if (leftList.size() > 0)
					leftMap.put(entry.getKey(), toIntArray(leftList));
				if (rightList.size() > 0)
					rightMap.put(entry.getKey(), toIntArray(rightList));
			}
		}

		Data leftData = new DataMR(leftLabels, leftMap);
		Data rightData = new DataMR(rightLabels, rightMap);
		// System.out.printf("split = %d(%d) | %d(%d)\n" ,
		// leftData.getNumExamples(), leftData.getMajority(),
		// rightData.getNumExamples(), rightData.getMajority());
		return new DataPair(leftData, rightData);
	}

	@SuppressWarnings("unchecked")
	private void computeSplitMR(HashSet<Integer> index, TreeMap<Integer, int[]> leftMap, TreeMap<Integer, int[]> rightMap) {
		try {

			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!Hadoop Split!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!Hadoop Split!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!Hadoop Split!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Path path = new Path("index");
			ObjectOutputStream os = new ObjectOutputStream(fs.create(path));
			os.writeObject(index);
			os.close();
			fs.deleteOnExit(path);

			// write <featID, list> into sequence file
			Path dataPath = new Path("input.dat");
			fs.deleteOnExit(dataPath);
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, dataPath, IntWritable.class, Text.class);
			for (Map.Entry<Integer, int[]> entry : data.entrySet()) {
				IntWritable key = new IntWritable(entry.getKey().intValue());
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				os = new ObjectOutputStream(baos);
				os.writeObject(entry.getValue());
				os.close();

				Text value = new Text(encode.encode(baos.toByteArray()));
				writer.append(key, value);

				/*
				 * // try to decode ObjectInputStream ois = new
				 * ObjectInputStream(new
				 * ByteArrayInputStream(decode.decodeBuffer(value.toString())));
				 * int[] a = (int[])ois.readObject(); for (int i=0; i<a.length;
				 * i++) System.out.printf("%d ",a[i]); System.out.println("");
				 */
			}
			writer.close();

			DistributedCache.addCacheFile(new URI("index#localindex"), conf);
			DistributedCache.createSymlink(conf);

			Job job = new Job(conf, "feature split calculation");
			job.setJarByClass(DataMR.class);
			job.setMapperClass(FeatureMapper.class);
			job.setCombinerClass(Reducer.class);
			job.setReducerClass(Reducer.class);
			job.setNumReduceTasks(1);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job, new Path("input.dat"));
			FileOutputFormat.setOutputPath(job, new Path("outfeat"));
			job.waitForCompletion(true);

			Path inFile = new Path("outfeat/part-r-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);

			IntWritable key = new IntWritable();
			Text value = new Text();
			while (reader.next(key, value)) {
				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decode.decodeBuffer(value.toString())));
				ArrayList<Integer> leftList = (ArrayList<Integer>) ois.readObject();
				ArrayList<Integer> rightList = (ArrayList<Integer>) ois.readObject();
				ois.close();

				if (leftList.size() > 0)
					leftMap.put(key.get(), toIntArray(leftList));
				if (rightList.size() > 0)
					rightMap.put(key.get(), toIntArray(rightList));
			}
			reader.close();

			fs.delete(new Path("outfeat"), true);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public static class FeatureMapper extends Mapper<IntWritable, Text, IntWritable, Text> {

		static private HashSet<Integer> index = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (index == null) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				// ObjectInputStream input = new ObjectInputStream(fs.open(new
				// Path("localdata")));
				ObjectInputStream input = new ObjectInputStream(new FileInputStream("localindex"));
				try {
					index = (HashSet<Integer>) input.readObject();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				input.close();
			}
		}

		@Override
		protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {

			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decode.decodeBuffer(value.toString())));
			int[] examples = null;
			try {
				examples = (int[]) ois.readObject();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			ois.close();

			ArrayList<Integer> leftList = new ArrayList<Integer>();
			ArrayList<Integer> rightList = new ArrayList<Integer>();
			for (int j = 0; j < examples.length; j++) {
				if (index.contains(examples[j])) {
					leftList.add(examples[j]);
				} else {
					rightList.add(examples[j]);
				}
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(baos);
			os.writeObject(leftList);
			os.writeObject(rightList);
			os.close();

			Text outValue = new Text(encode.encode(baos.toByteArray()));
			context.write(key, outValue);
		}
	}

}
