package simjoin.core.exec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ExecUtils {
	
	public static final String FILENAME_SUCC = "_SUCCESS_SIMJOIN_EXEC";
	
	public static boolean isExecSuccess(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		return fs.exists(new Path(path, FILENAME_SUCC));
	}

	public static void setExecSuccess(Configuration conf, Path path)
			throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		path = new Path(path, FILENAME_SUCC);
		fs.create(path, true).close();
	}
}
