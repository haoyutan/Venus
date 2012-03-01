package simjoin.spatial;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import simjoin.core.ItemWritable;
import simjoin.core.handler.ItemBuildHandler;

public class TextRegionItemBuildHandler extends
		ItemBuildHandler<LongWritable, Text, RegionItemWritable> {
	
	public TextRegionItemBuildHandler() {
		super(RegionItemWritable.class);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void resetItem(ItemWritable item, LongWritable key,
			Text value, int mask) {
		
		RegionItemWritable regionItem = (RegionItemWritable) item;
		
		String line = value.toString();
		String[] fields = line.split(",");
		
		regionItem.setId(new LongWritable(Long.parseLong(fields[0])));
		
		int numPoints = Integer.parseInt(fields[1]);
		double x[] = new double[numPoints];
		double y[] = new double[numPoints];
		double xMin = Double.POSITIVE_INFINITY;
		double xMax = Double.NEGATIVE_INFINITY;
		double yMin = Double.POSITIVE_INFINITY;
		double yMax = Double.NEGATIVE_INFINITY;
		
		for (int i = 0; i < numPoints; i++) {
			x[i] = Double.parseDouble(fields[i * 2 + 2]);
			y[i] = Double.parseDouble(fields[i * 2 + 3]);
			xMin = Math.min(xMin, x[i]);
			xMax = Math.max(xMax, x[i]);
			yMin = Math.min(yMin, y[i]);
			yMax = Math.max(yMax, y[i]);
		}
		
		RegionWritable region = new RegionWritable();
		region.setNumPoints(numPoints);
		region.setX(x);
		region.setY(y);
		regionItem.setPayload(region);

		MbrWritable mbr = new MbrWritable();
		mbr.setxMin(xMin);
		mbr.setxMax(xMax);
		mbr.setyMin(yMin);
		mbr.setyMax(yMax);
		regionItem.setSignature(mbr);
	}
}
