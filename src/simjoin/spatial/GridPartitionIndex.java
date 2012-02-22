package simjoin.spatial;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.core.SimJoinConf;
import simjoin.core.SimJoinUtils;
import simjoin.core.handler.ItemBuildHandler;

public class GridPartitionIndex extends Configured implements Tool {
	
	public static final String CK_INDEX_FILE = "simjoin.spatial.gridindex.file";
	
	public static final String DEFAULT_INDEX_DIRNAME = "_GridIndex";
	
	public static GridIndex loadGridIndex(Configuration conf) throws IOException {
		String indexPath = conf.get(CK_INDEX_FILE, null);
		if (indexPath == null)
			throw new RuntimeException("Must specify " + CK_INDEX_FILE + ".");
		Path indexFile = new Path(indexPath, "part-r-00000");
		FileSystem fs = indexFile.getFileSystem(conf);
		DataInputStream in = fs.open(indexFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String indexText = reader.readLine();
		reader.close();
		return new GridIndex(indexText);
	}

	public static class GridPartitionIndexMapper<KEYIN, VALUEIN> extends
			Mapper<KEYIN, VALUEIN, IntWritable, DoubleWritable> {

		private ItemBuildHandler<KEYIN, VALUEIN, ?> itemBuildHandler;
		
		private double xMin, xMax, yMin, yMax;
		
		private RegionItemWritable regionItem;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			itemBuildHandler = SimJoinUtils.createItemBuildHandler(conf);
			itemBuildHandler.setup(conf);
			regionItem = (RegionItemWritable) itemBuildHandler.createItem();
			
			xMin = Double.POSITIVE_INFINITY;
			xMax = Double.NEGATIVE_INFINITY;
			yMin = Double.POSITIVE_INFINITY;
			yMax = Double.NEGATIVE_INFINITY;
		}

		@Override
		protected void map(KEYIN key, VALUEIN value, Context context)
				throws IOException, InterruptedException {
			itemBuildHandler.resetItem(regionItem, key, value);
			MbrWritable mbr = regionItem.getSignature();
			
			xMin = Math.min(xMin, mbr.getxMin());
			xMax = Math.max(xMax, mbr.getxMax());
			yMin = Math.min(yMin, mbr.getyMin());
			yMax = Math.max(yMax, mbr.getyMax());
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new IntWritable(0), new DoubleWritable(xMin));
			context.write(new IntWritable(1), new DoubleWritable(xMax));
			context.write(new IntWritable(2), new DoubleWritable(yMin));
			context.write(new IntWritable(3), new DoubleWritable(yMax));
			
			Configuration conf = context.getConfiguration();
			itemBuildHandler.cleanup(conf);
			super.cleanup(context);
		}
	}
	
	public static class GridPartitionIndexReducer extends
			Reducer<IntWritable, DoubleWritable, Text, NullWritable> {
		
		private double xMin, xMax, yMin, yMax;
		
		@Override
		protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			int idx = key.get();
			if (idx == 0) {
				xMin = Double.POSITIVE_INFINITY;
				for (DoubleWritable value : values)
					xMin = Math.min(xMin, value.get());
			} else if (idx == 1) {
				xMax = Double.NEGATIVE_INFINITY;
				for (DoubleWritable value : values)
					xMax = Math.max(xMax, value.get());
			} else if (idx == 2) {
				yMin = Double.POSITIVE_INFINITY;
				for (DoubleWritable value : values)
					yMin = Math.min(yMin, value.get());
			} else if (idx == 3) {
				yMax = Double.NEGATIVE_INFINITY;
				for (DoubleWritable value : values)
					yMax = Math.max(yMax, value.get());
			} else
				throw new RuntimeException("Impossible event happens");
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int numStrips = conf.getInt("simjoin.spatial.gridindex.num_strips", 10);
			
			double width = xMax - xMin;
			double height = yMax - yMin;
			xMin -= width * 0.001;
			xMax += width * 0.001;
			yMin -= height * 0.001;
			yMax += height * 0.001;

			context.write(
					new Text(GridIndex.getIndexText(numStrips, xMin, xMax,
							yMin, yMax)), NullWritable.get());

			super.cleanup(context);
		}
	}

	public static class GridIndex {
		
		private int numStrips;
		
		private double xMin, xMax, yMin, yMax;
		
		private double width, height;
		
		public static String getIndexText(int numStrips, double xMin,
				double xMax, double yMin, double yMax) {
			return "" + numStrips + "," + xMin + "," + xMax
					+ "," + yMin + "," + yMax;
		}
		
		public GridIndex(String indexText) {
			String[] fields = indexText.split(",");
			numStrips = Integer.parseInt(fields[0]);
			xMin = Double.parseDouble(fields[1]);
			xMax = Double.parseDouble(fields[2]);
			yMin = Double.parseDouble(fields[3]);
			yMax = Double.parseDouble(fields[4]);
			width = xMax - xMin;
			height = yMax - yMin;
		}
		
		public List<Integer> getPartitions(MbrWritable mbr) {
			double x1 = mbr.getxMin();
			double x2 = mbr.getxMax();
			double y1 = mbr.getyMin();
			double y2 = mbr.getyMax();
			
			int left = (int) ((x1 - xMin) * numStrips / width);
			int right = (int) ((x2 - xMin) * numStrips / width);
			int bottom = (int) ((y1 - yMin) * numStrips / height);
			int top = (int) ((y2 - yMin) * numStrips / height);
			
			ArrayList<Integer> pids = new ArrayList<Integer>();
			for (int i = left; i <= right; i++)
				for (int j = bottom; j <= top; j++)
					pids.add(j * numStrips + i);
			
			return pids;
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName("Step-0-Preprocessing");
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(GridPartitionIndexMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);
		job.setReducerClass(GridPartitionIndexReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0],
				DEFAULT_INDEX_DIRNAME));

		SimJoinConf.setItemClass(job.getConfiguration(),
				RegionItemWritable.class);
		SimJoinConf.setItemBuildHandlerClass(job.getConfiguration(),
				TextRegionItemBuildHandler.class, true);
		
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GridPartitionIndex(), args);
	}
}
