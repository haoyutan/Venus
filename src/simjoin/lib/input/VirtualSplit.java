package simjoin.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class VirtualSplit extends InputSplit implements Writable {
	
	private Text info = new Text();
	
	private long length;
	
	public VirtualSplit() {
	}
	
	public VirtualSplit(String info, long length) {
		this.info.set(info);
		this.length = length;
	}
	
	public String getInfo() {
		return info.toString();
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		length = in.readLong();
		info.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(length);
		info.write(out);
	}
}
