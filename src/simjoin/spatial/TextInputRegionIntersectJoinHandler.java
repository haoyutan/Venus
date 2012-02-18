package simjoin.spatial;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public abstract class TextInputRegionIntersectJoinHandler extends
		RegionIntersectJoinHandler<LongWritable, Text> {

	@Override
	protected void resetItem(RegionItemWritable item, LongWritable key,
			Text value) {
		
		String line = value.toString();
		String[] fields = line.split(",");
		
		item.setId(new IntWritable(Integer.parseInt(fields[0])));
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
		item.setPayload(region);

		MbrWritable mbr = new MbrWritable();
		mbr.setxMin(xMin);
		mbr.setxMax(xMax);
		mbr.setyMin(yMin);
		mbr.setyMax(yMax);
		item.setSignature(mbr);
	}
}
