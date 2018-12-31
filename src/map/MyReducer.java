package map;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		Double pr = 0.0;
		String outLine = "";
		for (Text val : values) {
			if (val.toString().substring(0, 1).equals("*"))  // 说明后面是贡献值
				pr += Double.parseDouble(val.toString().substring(1));
			else if(val.toString().substring(0, 1).equals("@"))  // 后面是出链
				outLine += val.toString().substring(1);
		}
		pr = pr * Driver.alpha + (1 - Driver.alpha) / Driver.pageSize;
		context.write(_key,new Text(pr.toString() + " " + outLine));
	}
}
