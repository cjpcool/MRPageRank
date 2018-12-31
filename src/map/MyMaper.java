package map;

import java.io.IOException;

import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.Mapper;  

public class MyMaper extends Mapper<Object, Text, Text, Text> {
/*
 * (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
 * ÿһ�е����  key:��kv��id����title����url, value: ����һ��page�����page�ĸ���
 * 
 */
	final static double alpha = 0.85;
	@Override
	public void map(Object ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String line = ivalue.toString().trim();  
		String[] split = line.split(" ");// split[0]�Ǹ���ҳid, split[1]�Ǹ���ҳpr, �����Ǹ���ҳ����
		Double pr = Double.parseDouble(split[1]);
		int outCount = split.length - 2;  // ��ȥǰ��������ֵ ʣ�³�����
		if(pr.equals(0)){  // ����ǵ�һ�μ��� ��ʼ��prΪ ����ϵ��alpah(ʵ���Ͽ���������� ��Ϊ��������)
			pr = alpha;
		}
		String outLine = "@";
		System.out.println("**"+line);
		Double value = pr/outCount;
		for(int i = 2; i < outCount; i++){
			context.write(new Text(split[i]), new Text("*"+value.toString()));  // id *��������ֵ
			outLine += split[i] + " ";
		}
		context.write(new Text(split[0]), new Text(outLine));  // id @����1 ����2 ����3 ...
	}
}
