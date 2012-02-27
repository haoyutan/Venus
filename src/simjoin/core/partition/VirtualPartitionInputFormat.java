package simjoin.core.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import simjoin.core.SimJoinConf;

public class VirtualPartitionInputFormat extends
		InputFormat<VirtualPartition, VirtualPartition> {

	public static final String CK_INPUTDIR = "simjoin.core.partition.inputformat.inputdir";
	
	public static Path getInputDir(Configuration conf) {
		return new Path(conf.get(CK_INPUTDIR));
	}
	
	public static void setInputDir(Configuration conf, Path inputDir) throws IOException {
		SimJoinConf.setPath(conf, CK_INPUTDIR, inputDir);
	}
	
	public static Path getInputDir(Job job) {
		return getInputDir(job.getConfiguration());
	}
	
	public static void setInputDir(Job job, Path inputDir) throws IOException {
		setInputDir(job.getConfiguration(), inputDir);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Configuration conf = context.getConfiguration();
		Path inputDir = getInputDir(conf);
		FileSystem fs = inputDir.getFileSystem(conf);
		FileStatus[] statusList = fs.listStatus(inputDir, new TaskFilePathFilter());
		for (FileStatus status : statusList)
			if (!status.isDir())
				splits.add(new FileSplit(status.getPath(), 0, -1, null));
		return splits;
	}

	@Override
	public RecordReader<VirtualPartition, VirtualPartition> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new VirtualPartitionRecordReader();
	}
	
	private static class TaskFilePathFilter implements PathFilter {
		@Override
		public boolean accept(Path path) {
			return !path.getName().startsWith("_");
		}
	}
}
