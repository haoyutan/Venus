package simjoin.spatial;

import simjoin.core.SimJoinHandler;

public abstract class RegionIntersectJoinHandler<KEYIN, VALUEIN> extends
		SimJoinHandler<KEYIN, VALUEIN, RegionItemWritable> {

	public RegionIntersectJoinHandler() {
		super(RegionItemWritable.class);
	}
}
