package simjoin.spatial;

import org.apache.hadoop.io.IntWritable;

import simjoin.core.ItemWritable;

public class RegionItemWritable extends ItemWritable<IntWritable, RegionWritable, MbrWritable> {
	
	public RegionItemWritable() {
		super(IntWritable.class, RegionWritable.class, MbrWritable.class, true);
	}
}
