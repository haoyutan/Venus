package simjoin.spatial;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import simjoin.core.ItemWritable;
import simjoin.core.handler.ItemPartitionHandler;
import simjoin.spatial.GridPartitionIndex.GridIndex;

public class GridRegionItemPartitionHandler extends
		ItemPartitionHandler<RegionItemWritable> {

	public static final String DEFAULT_INDEX_DIRNAME = GridPartitionIndex.DEFAULT_INDEX_DIRNAME;
	
	public static void setGridIndexFile(Configuration conf, Path indexPath) {
		conf.set(GridPartitionIndex.CONF_INDEX_FILE, indexPath.toString());
	}

	public GridRegionItemPartitionHandler() {
		super(RegionItemWritable.class);
	}
	
	private GridIndex gridIndex;

	@Override
	public void setup(Configuration conf) throws IOException {
		gridIndex = GridPartitionIndex.loadGridIndex(conf);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public List<Integer> getPartitions(ItemWritable item) {
		RegionItemWritable regionItem = (RegionItemWritable) item;
		return gridIndex.getPartitions(regionItem.getSignature());
	}
}
