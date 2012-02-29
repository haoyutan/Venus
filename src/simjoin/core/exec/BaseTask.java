package simjoin.core.exec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import simjoin.core.SimJoinConf;

public abstract class BaseTask extends Configured implements Tool {
	
	public static final String FILENAME_SUCC = "_SUCCESS_SIMJOIN_EXEC";
	
	public static final String FILENAME_EXEC_PLAN = "_EXEC_PLAN";
	
	public static final String CK_EXEC_TASK_INPUT_PATH = "simjoin.core.exec.task.input_path";
	
	public static final String CK_EXEC_TASK_OUTPUT_PATH = "simjoin.core.exec.task.output_path";
	
	// CK_EXEC_INPUT_PATH
	public static void setTaskInputPath(Configuration conf, Path inputPath) throws IOException {
		SimJoinConf.setPath(conf, CK_EXEC_TASK_INPUT_PATH, inputPath);
	}
	
	public static Path getTaskInputPath(Configuration conf) {
		String pathStr = conf.get(CK_EXEC_TASK_INPUT_PATH);
		return (pathStr != null ? new Path(conf.get(CK_EXEC_TASK_INPUT_PATH))
				: null);
	}
	
	// CK_EXEC_OUTPUT_PATH
	public static void setTaskOutputPath(Configuration conf, Path inputPath) throws IOException {
		SimJoinConf.setPath(conf, CK_EXEC_TASK_OUTPUT_PATH, inputPath);
	}
	
	public static Path getTaskOutputPath(Configuration conf) {
		return new Path(conf.get(CK_EXEC_TASK_OUTPUT_PATH));
	}

	// FILENAME_SUCC
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
	
	protected Path taskInputPath, taskOutputPath;
	
	public BaseTask(Configuration conf) {
		super(conf);
	}

	@Override
	public final int run(String[] args) throws Exception {
		Configuration conf = getConf();
		taskInputPath = getTaskInputPath(conf);
		taskOutputPath = getTaskOutputPath(conf);
		
		if (BaseTask.isExecSuccess(conf, taskOutputPath)) {
			Logger.getLogger(getClass()).info("Found saved results. Skip.");
			return 0;
		}
		
		loadExecPlan();
		Logger.getLogger(getClass()).info("Task begin.");
		int ret = runTask(args);
		if (ret == 0) {
			saveExecPlan();
			BaseTask.setExecSuccess(getConf(), taskOutputPath);
			Logger.getLogger(getClass()).info("Task finished.");
		}
		return ret;
	}
	
	protected abstract int runTask(String[] args) throws Exception;
	
	private void loadExecPlan() throws IOException {
		if (taskInputPath == null) // This is the first task.
			return;
		
		Configuration conf = getConf();
		Path prevPlanFile = new Path(taskInputPath, FILENAME_EXEC_PLAN);
		FileSystem fs = prevPlanFile.getFileSystem(conf);
		if (fs.exists(prevPlanFile) && fs.isFile(prevPlanFile)) {
			Configuration prevPlan = new Configuration();
			DataInputStream in = fs.open(prevPlanFile);
			prevPlan.readFields(in);
			in.close();
			
			Map<String, String> kvMap = conf.getValByRegex("simjoin");
			for (Map.Entry<String, String> entry : kvMap.entrySet())
				prevPlan.set(entry.getKey(), entry.getValue());
			setConf(prevPlan);
		}
	}
	
	private void saveExecPlan() throws IOException {
		// save binary
		Configuration conf = getConf();
		Path confFile = new Path(taskOutputPath, FILENAME_EXEC_PLAN);
		FileSystem fs = confFile.getFileSystem(conf);
		DataOutputStream out = fs.create(confFile, true);
		conf.write(out);
		out.close();
		
		// save xml
		Path xmlFile = new Path(taskOutputPath, FILENAME_EXEC_PLAN + ".xml");
		out = fs.create(xmlFile, true);
		conf.writeXml(out);
		out.close();
	}
}
