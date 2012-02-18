package simjoin.spatial;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import simjoin.lib.input.VirtualInputFormat;
import simjoin.lib.input.VirtualRecordReader;

public class VirtualTextRegionInputFormat extends
		VirtualInputFormat<LongWritable, Text> {

	public static class VirtualTextRegionRecordReader extends
			VirtualRecordReader<LongWritable, Text> {

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			super.initialize(split, context);
			key = new LongWritable();
			value = new Text();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (curRecords == totalRecords)
				return false;
			
			key.set(-1);
			value.set("" + curRecords + ",3,0,0,1,0,1,1");
			++curRecords;
			return true;
		}
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new VirtualTextRegionRecordReader();
	}
}
