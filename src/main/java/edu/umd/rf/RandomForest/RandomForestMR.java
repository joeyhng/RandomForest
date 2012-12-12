package edu.umd.rf.RandomForest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class RandomForestMR extends RandomForest {

	public RandomForestMR(){
		super(Driver.NUMTREE, Driver.TREEDEPTH);
	}
	
	public RandomForestMR(int numTrees, int maxDepth) {
		super(numTrees, maxDepth);
	}

	public void train(Data data) {

		try {

			Configuration conf = new Configuration();
			conf.setInt("maxDepth", getMaxDepth());
			conf.setInt("numSplits", Driver.NUMSPLIT);
			conf.setInt("numTrees", numTrees);

			System.err.println("!!!!!!!!!!!!! writing data !!!!!!!!!!!!!!!!!!!!!");
			FileSystem fs = FileSystem.get(conf);

			Path path = new Path("data");
			ObjectOutputStream os = new ObjectOutputStream(fs.create(path));
			os.writeObject(data);
			os.close();
			fs.deleteOnExit(path);

			DistributedCache.addCacheFile(new URI("data#localdata"), conf);
			DistributedCache.createSymlink(conf);

			System.err.println("!!!!!!!!!!!!!!!!!!!!! finish writing data !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

			Job job = new Job(conf, "random forest");

			job.setJarByClass(RandomForestMR.class);
			job.setMapperClass(TreeMapper.class);
			job.setReducerClass(Reducer.class);
			job.setNumReduceTasks(1);

			job.setInputFormatClass(RandomForestInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Tree.class);

			FileOutputFormat.setOutputPath(job, new Path("out"));
			job.waitForCompletion(true);

			Path inFile = new Path("out/part-r-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! try to read back the sequnece file");
			try {
				LongWritable key = new LongWritable();
				Tree tmpTree = new Tree();
				while (reader.next(key, tmpTree)) {
					System.err.println("reading!!!! : " + key.get());
					System.err.println("tree: " + tmpTree);
					this.trees[(int) key.get()] = tmpTree;
				}
			} finally {
				reader.close();
			}

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

	public static class TreeMapper extends Mapper<LongWritable, LongWritable, LongWritable, Tree> {

		static private Data data = null;
		static private int maxTreeDepth;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (data == null) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				// ObjectInputStream input = new ObjectInputStream(fs.open(new
				// Path("localdata")));
				ObjectInputStream input = new ObjectInputStream(new FileInputStream("localdata"));
				try {
					data = (Data) input.readObject();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				input.close();
			}
			maxTreeDepth = context.getConfiguration().getInt("maxDepth", 10);
		}

		@Override
		protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
			System.out.println("mapper!!!!!!!!!!!!!!!!! max depth = " + context.getConfiguration().getInt("maxDepth", 0));

			Tree tree = new Tree(maxTreeDepth);
			tree.train(data);
			context.write(key, tree);
		}
	}

	/*
	 * public static class TreeReducer extends Reducer <LongWritable, Tree,
	 * LongWritable, Tree>{
	 * 
	 * @Override protected void cleanup(Context context) throws IOException,
	 * InterruptedException { Path outFile =
	 * new Path("reduce-out"); FileSystem fs =
	 * FileSystem.get(context.getConfiguration()); SequenceFile.Writer writer =
	 * SequenceFile.createWriter(fs, context.getConfiguration(), outFile,
	 * LongWritable.class, Tree.class, CompressionType.NONE); writer.append(new
	 * LongWritable(), ); writer.close(); }
	 * 
	 * @Override protected void reduce(LongWritable arg0, Iterable<Tree> arg1,
	 * Context arg2) throws IOException, InterruptedException {
	 * 
	 * super.reduce(arg0, arg1, arg2); }
	 * 
	 * }
	 */
	

	public static class RandomForestInputFormat extends InputFormat<LongWritable, LongWritable> {

		@Override
		public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			return new RandomForestRecordReader((RandomForestInputSplit) split);
		}

		@Override
		public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!! trying to get split !!!!!!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			// int numSplits = jobContext.getConfiguration().getInt("numSplits", 1);
			// int numTrees = jobContext.getConfiguration().getInt("numTrees", 1);
			int numSplits = Driver.NUMSPLIT;
			int numTrees = Driver.NUMTREE;
			System.out.println("Number of trees = " + numTrees);
			List<InputSplit> splits = new ArrayList<InputSplit>();
			int size = (numTrees + numSplits - 1) / numSplits;
			for (int i = 0; i < numSplits; i++) {
				splits.add(new RandomForestInputSplit(i * size, Math.min(numTrees, (i + 1) * size)));
				System.err.println("range generated: " + (i * size) + " " + Math.min(numTrees, (i + 1) * size));
			}
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!! finish generating split !!!!!!!!!!!!");
			System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

			return splits;
		}
	}
	

	public static class RandomForestRecordReader extends RecordReader<LongWritable, LongWritable> {

		private int start;
		private int end;
		private int now;

		private LongWritable key;
		private LongWritable value;

		public RandomForestRecordReader(RandomForestInputSplit split) {
			this.start = split.getStart();
			this.end = split.getEnd();
			this.now = split.getStart();
		}

		@Override
		public void close() throws IOException {}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public LongWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (this.now == this.end) {
				return 0;
			} else {
				return Math.min(1, (this.now - this.start) / (float) (this.end - this.start));
			}
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
			RandomForestInputSplit split = (RandomForestInputSplit) inputSplit;
			this.start = split.getStart();
			this.end = split.getEnd();
			this.now = split.getStart();

			this.key = new LongWritable(this.now);
			this.value = new LongWritable(this.now);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
			System.err.println("!!!!!!!!!!! trying to get next key value !!!!!!!");
			System.err.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

			if (this.now < this.end) {
				if (key == null)
					System.err.println("============== key is null");
				else
					System.err.println("============== key " + key.toString());

				key.set(this.now);
				value.set(this.now);
				this.now++;
				return true;
			}
			return false;
		}

	}

	public static class RandomForestInputSplit extends InputSplit implements Writable {

		private int start;
		private int end;

		public RandomForestInputSplit() {}

		public RandomForestInputSplit(int start, int end) {
			this.start = start;
			this.end = end;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return (this.end - this.start) * 8;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {};
		}

		public int getStart() {
			return start;
		}

		public int getEnd() {
			return end;
		}

		public void readFields(DataInput in) throws IOException {
			this.start = in.readInt();
			this.end = in.readInt();
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(start);
			out.writeInt(end);
		}
	}



}
