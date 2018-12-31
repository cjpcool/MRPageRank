package map;

import java.io.IOException;

import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.Mapper;  

public class MyMaper extends Mapper<Object, Text, Text, Text> {
/*
 * (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 * 每一行的输出  key:该kv的id或者title或者url, value: 从上一个page到达该page的概率
 * 
 */
	final static double alpha = 0.85;
	@Override
	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString().trim();  
		String[] split = line.split(" ");// split[0]是该网页id, split[1]是该网页pr, 其他是该网页出链
		Double pr = Double.parseDouble(split[1]);
		int outCount = split.length - 2;  // 减去前两个数的值 剩下出链数
		if(pr.equals(0)){  // 如果是第一次计算 初始化pr为 阻尼系数alpah(实际上可以是随机数 因为最后会收敛)
			pr = alpha;
		}
		String outLine = "@";
		System.out.println("**"+line);
		Double value = pr/outCount;
		for(int i = 2; i < outCount; i++){
			context.write(new Text(split[i]), new Text("*"+value.toString()));  // id *出链贡献值
			outLine += split[i] + " ";
		}
		context.write(new Text(split[0]), new Text(outLine));  // id @出链1 出链2 出链3 ...
	}
}
