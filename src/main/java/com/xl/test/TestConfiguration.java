package com.xl.test;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestConfiguration extends Configured implements Tool{

	static{
		Configuration.addDefaultResource("core-site.xml");
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TestConfiguration(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		for (Entry<String, String> entry : conf) {
			if(entry.getKey().equals("hadoop.tmp.dir")){
			System.out.println( entry.getKey() + "====" +entry.getValue());
			}
		}
		return 0;
	}

}
