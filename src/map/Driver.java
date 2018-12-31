package map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Driver {
	public final static float alpha = 0.85f;
	public final static int pageSize = 59138;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator"," "); //设置以空格为分隔符
		String inPath = "hdfs://192.168.56.101:9000/user/hadoop/testdata/reduce_50000.txt";
		String outPath = "hdfs://192.168.56.101:9000/user/hadoop/output/out";
		for (int i = 0; i < 10; i++) {
			Job job = Job.getInstance(conf, "bigdata");
			job.setJarByClass(Driver.class);
			// TODO: specify a mapper
			job.setMapperClass(MyMaper.class);
			// TODO: specify a reducer
			job.setReducerClass(MyReducer.class);

			// TODO: specify output types
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);

			// TODO: specify input and output DIRECTORIES (not files)
			FileInputFormat.addInputPath(job, new Path(inPath)); // 将此添加到作业输入列表中
			FileOutputFormat.setOutputPath(job, new Path(outPath));

			inPath = outPath;
			outPath = outPath + i;
			if (!job.waitForCompletion(true))
				return;
		}
	}

}
