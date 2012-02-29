package simjoin.spatial;

import org.apache.hadoop.io.LongWritable;

import simjoin.core.ItemWritable;

public class RegionItemWritable extends
		ItemWritable<LongWritable, RegionWritable, MbrWritable> {
	
	public RegionItemWritable() {
		super(LongWritable.class, RegionWritable.class, MbrWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(ItemWritable o) {
		return getId().compareTo(o.getId());
	}
}
