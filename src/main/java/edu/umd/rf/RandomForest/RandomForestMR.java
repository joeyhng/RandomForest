package edu.umd.rf.RandomForest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class RandomForestMR extends RandomForest {

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
				while (reader.next(key, tmpTree)){
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
		}

		/*
		 * for (int i = 0; i < this.numTrees; i++){
		 * System.err.printf("training %dth tree\n", i); this.trees[i] = new
		 * Tree(maxDepth); this.trees[i].train(data); }
		 */
	}

	public static class TreeMapper extends Mapper<LongWritable, LongWritable, LongWritable, Tree> {

		static private Data data = null;
		static private int maxTreeDepth;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			if (data == null) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				ObjectInputStream input = new ObjectInputStream(fs.open(new Path("data")));
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
	 * InterruptedException { // TODO Auto-generated method stub Path outFile =
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

}
