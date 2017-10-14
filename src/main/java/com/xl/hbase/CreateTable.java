package com.xl.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class CreateTable {

	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		HTable table = new HTable(configuration, "testxl");
		HBaseAdmin admin = new HBaseAdmin(configuration);

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test2"));
		
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));
		admin.createTable(tableDescriptor);
		
		HTableDescriptor[] tableDescriptors =admin.listTables();
		
		for (int i = 0; i < tableDescriptors.length; i++) {
			System.out.println(tableDescriptors[i].getNameAsString());
		}
		
		System.out.println(admin.isTableDisabled("xl"));
	}

}
