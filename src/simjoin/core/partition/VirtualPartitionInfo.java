package simjoin.core.partition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VirtualPartitionInfo {
	
	public static final String FILENAME_SUMMARY = "_partition_summary.csv";
	
	public static final String TASK_FILENAME_PREFIX = "T-";

	private VirtualPartitionID id;
	
	private Path partitionFile;
	
	private long start;
	
	private long length;
	
	private long numRecords;
	
	public VirtualPartitionInfo(VirtualPartitionID id, Path partitionFile,
			long start, long length, long numRecords) {
		this.id = id;
		this.partitionFile = partitionFile;
		this.start = start;
		this.length = length;
		this.numRecords = numRecords;
	}
	
	public VirtualPartitionInfo() {
		this(null);
	}
	
	public VirtualPartitionInfo(VirtualPartitionID id) {
		this(id, null, 0, -1, -1);
	}

	public VirtualPartitionID getId() {
		return id;
	}

	public void setId(VirtualPartitionID id) {
		this.id = id;
	}

	public Path getPartitionFile() {
		return partitionFile;
	}

	public void setPartitionFile(Path partitionFile) {
		this.partitionFile = partitionFile;
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public long getNumRecords() {
		return numRecords;
	}

	public void setNumRecords(long numRecords) {
		this.numRecords = numRecords;
	}
	
	@Override
	public String toString() {
		return id.toString() + "," + partitionFile + "," + start + "," + length
				+ "," + numRecords;
	}
	
	public boolean isSplitable() {
		return id.isPhysicalPartition();
	}
	
	public List<VirtualPartitionInfo> getSplits(int numSplits) {
		return getSplits(numSplits, 2000000, 1.1);
	}

	public List<VirtualPartitionInfo> getSplits(int numSplits, long minSplitSize,
			double splitSlop) {
		if (!isSplitable() || start != 0 || length < 0)
			throw new IllegalArgumentException("This virtual partition is unsplitable.");
		if (numSplits <= 0)
			throw new IllegalArgumentException("numSplits must be positive.");
		
		List<VirtualPartitionInfo> splitInfoList = new ArrayList<VirtualPartitionInfo>();
		long splitSize = (long) Math.max(minSplitSize, Math.ceil(length / numSplits));
		long bytesRemaining = length;
		long mainId = id.getMainId();
		long nextSubId = 0;
		while (((double) bytesRemaining) / splitSize > splitSlop) {
			splitInfoList.add(new VirtualPartitionInfo(new VirtualPartitionID(
					mainId, nextSubId), partitionFile, length - bytesRemaining,
					splitSize, numRecords * splitSize / length));
			bytesRemaining -= splitSize;
			++nextSubId;
		}
		if (bytesRemaining != 0)
			splitInfoList.add(new VirtualPartitionInfo(new VirtualPartitionID(
					mainId, nextSubId), partitionFile, length - bytesRemaining,
					bytesRemaining, numRecords * bytesRemaining / length));

		return splitInfoList;
	}

	public static void writeVirtualPartitionInfo(Configuration conf, Path path,
			Collection<VirtualPartitionInfo> vpInfoSet, boolean overwrite)
			throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path) && fs.getFileStatus(path).isDir())
			path = new Path(path, FILENAME_SUMMARY);
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(fs.create(
				path, overwrite)));
		writer.println("id,file,start,length,numRecords");
		for (VirtualPartitionInfo vpInfo : vpInfoSet)
			writer.printf("%s,%s,%d,%d,%d\n", vpInfo.getId(),
					vpInfo.getPartitionFile(), vpInfo.getStart(),
					vpInfo.getLength(), vpInfo.getNumRecords());
		writer.close();
	}

	public static Map<VirtualPartitionID, VirtualPartitionInfo> readVirtualPartitionInfo(
			Configuration conf, Path path, boolean forceAbsolutePath)
			throws IOException {
		HashMap<VirtualPartitionID, VirtualPartitionInfo> map = 
				new HashMap<VirtualPartitionID, VirtualPartitionInfo>();
		FileSystem fs = path.getFileSystem(conf);
		if (fs.getFileStatus(path).isDir())
			path = new Path(path, FILENAME_SUMMARY);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		String line = reader.readLine(); // consume the header
		while ((line = reader.readLine()) != null) {
			String[] fields = line.split(",");
			VirtualPartitionID vpId = VirtualPartitionID.createFromString(fields[0]);
			VirtualPartitionInfo vpInfo = new VirtualPartitionInfo(vpId);
			Path partitionFile = new Path(fields[1]);
			if (forceAbsolutePath && !partitionFile.isAbsolute())
				partitionFile = new Path(path.getParent(), partitionFile.getName());
			vpInfo.setPartitionFile(partitionFile);
			vpInfo.setStart(Long.parseLong(fields[2]));
			vpInfo.setLength(Long.parseLong(fields[3]));
			vpInfo.setNumRecords(Long.parseLong(fields[4]));
			map.put(vpId, vpInfo);
		}
		reader.close();
		return map;
	}
}
