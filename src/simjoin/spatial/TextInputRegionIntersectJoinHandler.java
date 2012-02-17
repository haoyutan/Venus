package simjoin.spatial;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextInputRegionIntersectJoinHandler extends
		RegionIntersectJoinHandler<LongWritable, Text> {

	@Override
	protected void resetItem(RegionItemWritable item, LongWritable key,
			Text value) {

		item.setId(new IntWritable((int) key.get()));
		
		String line = value.toString();
		String[] fields = line.split(",");
		
		int numPoints = Integer.parseInt(fields[0]);
		double x[] = new double[numPoints];
		double y[] = new double[numPoints];
		double xMin = Double.MAX_VALUE;
		double xMax = Double.MIN_VALUE;
		double yMin = Double.MAX_VALUE;
		double yMax = Double.MIN_VALUE;
		
		for (int i = 0; i < numPoints; i++) {
			x[i] = Double.parseDouble(fields[i * 2 + 1]);
			y[i] = Double.parseDouble(fields[i * 2 + 2]);
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
