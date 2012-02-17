package simjoin.spatial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.Writable;

public class MbrWritable implements Writable {
	
	private double xMin, xMax, yMin, yMax;

	public double getxMin() {
		return xMin;
	}

	public void setxMin(double xMin) {
		this.xMin = xMin;
	}

	public double getxMax() {
		return xMax;
	}

	public void setxMax(double xMax) {
		this.xMax = xMax;
	}

	public double getyMin() {
		return yMin;
	}

	public void setyMin(double yMin) {
		this.yMin = yMin;
	}

	public double getyMax() {
		return yMax;
	}

	public void setyMax(double yMax) {
		this.yMax = yMax;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(xMin);
		out.writeDouble(xMax);
		out.writeDouble(yMin);
		out.writeDouble(yMax);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		xMin = in.readDouble();
		xMax = in.readDouble();
		yMin = in.readDouble();
		yMax = in.readDouble();
	}
	
	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		pw.format("%f,%f,%f,%f", xMin, xMax, yMin, yMax);
		return sw.toString();
	}
}
