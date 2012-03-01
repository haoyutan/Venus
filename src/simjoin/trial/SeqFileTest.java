package simjoin.trial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simjoin.spatial.RegionItemWritable;

public class SeqFileTest extends Configured implements Tool {
	
	public int dump(Path file, long start, long end) throws IOException,
			InterruptedException {
		FileSplit split = new FileSplit(file, start, end, new String[0]);
		RecordReader<RegionItemWritable, NullWritable> reader = 
				new SequenceFileRecordReader<RegionItemWritable, NullWritable>();
		reader.initialize(split, new TaskAttemptContext(getConf(), new TaskAttemptID()));
		
		long numRecords = 0;
		while (reader.nextKeyValue()) {
//			RegionItemWritable key = (RegionItemWritable) reader
//					.getCurrentKey();
//			System.out.println(key.getId());
			++numRecords;
		}
		if (numRecords != 0)
			System.out.println("" + numRecords + "," + start + "," + end);
		reader.close();

		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Path file = new Path(args[0]);
		FileSystem fs = file.getFileSystem(conf);
		long posStep = Long.parseLong(args[1]);
		
		long fileLength = fs.getFileStatus(file).getLen();
		long end;
		for (end = posStep; end < fileLength; end += posStep)
			dump(file, end - posStep, end);
		dump(file, end - posStep, fileLength);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SeqFileTest(), args);
	}
}
