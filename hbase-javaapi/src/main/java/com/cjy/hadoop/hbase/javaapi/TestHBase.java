/*
 * @author caojiayao 2017年2月13日 下午5:45:31
 */
package com.cjy.hadoop.hbase.javaapi;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;  

/**
 * <p>
 * <P>
 * 
 * @author caojiayao
 * @version $Id: TestHBase.java, v 0.1 2017年2月13日 下午5:45:31 caojiayao Exp $
 */
public class TestHBase {

	public static Configuration configuration;

	public static final String filePath = "META-INF/hbase-site.xml";

	/**
	 * 初始化配置信息
	 */
	static {
		configuration = HBaseConfiguration.create();
		// 配置方式一:配置地址
//		configuration.set("hbase.master", "local1:16000");
		configuration.set("hbase.zookeeper.quorum", "local2");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
//		configuration.set("hbase.zookeeper.property.dataDir", "/home/hadoop/storage/zookeeper");

		// 配置方式二:读取文件
		// Path path = new Path(filePath);
		// configuration.addResource(path);
	}

	/**
	 * 创建表
	 * 
	 * @param tableName
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	public static void createTable(String tableName)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(configuration);
		if (admin.tableExists(tableName)) {
			System.out.println("表失效!");
			admin.disableTable(tableName);
			System.out.println("删除表!");
			admin.deleteTable(tableName);
		}
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		hTableDescriptor.addFamily(new HColumnDescriptor("cf1"));
		admin.createTable(hTableDescriptor);
		admin.close();
	}
	
	/** 
     * 初始化数据 
     * @param tableName 
     * @throws Exception 
     */  
    public static void initData(String tableName) throws Exception{  
        HTable table = new HTable(configuration, tableName) ;  
        for(int i=10;i<22;i++){  
            String ii = String.valueOf(i);  
            Put put = new Put(ii.getBytes()) ;  
            put.add("cf1".getBytes(), "column1".getBytes(), "the first column".getBytes()) ;  
            put.add("cf1".getBytes(), "column2".getBytes(), "the second column".getBytes()) ;  
            put.add("cf1".getBytes(), "column3".getBytes(), "the third column".getBytes()) ;  
            table.put(put) ;  
        }  
        table.close();  
    }  
      
    /** 
     * 删除一行数据 
     * @param tableName 表名 
     * @param rowKey rowkey 
     * @throws Exception 
     */  
    public static void deleteRow(String tableName,String rowKey) throws Exception{  
        HTable table = new HTable(configuration,tableName);  
        Delete delete = new Delete(rowKey.getBytes());  
        table.delete(delete);  
    }  
      
    /** 
     *  删除rowkey列表 
     * @param tableName 
     * @param rowKeys 
     * @throws Exception 
     */  
    public static void deleteRowKeys(String tableName, List<String> rowKeys) throws Exception  
    {  
        HTable table = new HTable(configuration, tableName) ;  
        List<Delete> deletes = new ArrayList<Delete>();  
        for(String rowKey:rowKeys){  
            Delete delete = new Delete(rowKey.getBytes());  
            deletes.add(delete);  
        }  
        table.delete(deletes);  
        table.close();  
    }  
      
    /** 
     * 根据rowkey获取所有column值 
     * @param tableName 
     * @param rowKey 
     * @throws Exception 
     */  
    public static void get(String tableName,String rowKey) throws Exception{  
        HTable table = new HTable(configuration, tableName) ;  
        Get get = new Get(rowKey.getBytes());  
        Result result = table.get(get);  
        for(KeyValue kv:result.raw()){  
            System.out.println("cf="+new String(kv.getFamily())+", columnName="+new String(kv.getQualifier())+", value="+new String(kv.getValue()));  
        }  
    }  
      
      
      
    /** 
     * 批量查询 
     * @param tableName 
     * @param startRow 
     * @param stopRow 
     * @throws Exception 
     * select column1,column2,column3 from test_table where id between ... and 
     */  
    public static void scan(String tableName, String startRow, String stopRow) throws Exception {  
        HTable table = new HTable(configuration, tableName);  
        Scan scan = new Scan () ;  
        scan.addColumn("cf1".getBytes(), "column1".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column2".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column3".getBytes()) ;  
          
        //rowkey>=a && rowkey<b  
        scan.setStartRow(startRow.getBytes());  
        scan.setStopRow(stopRow.getBytes());  
        ResultScanner scanner = table.getScanner(scan) ;  
          
        for(Result result : scanner)  
        {  
            for(KeyValue keyValue : result.raw())  
            {  
                System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));  
            }  
        }  
          
    }  
      
    /** 
     * 单条件查询：测试SingleColumnValueFilter过滤器 
     * @param tableName 
     * @param columnValue 
     * @throws Exception 
     * LESS  < 
        LESS_OR_EQUAL <= 
        EQUAL = 
        NOT_EQUAL <> 
        GREATER_OR_EQUAL >= 
        GREATER > 
        NO_OP no operation 
     */  
    public static void testSingleColumnValueFilter(String tableName, String columnValue) throws Exception  
    {  
        HTable table = new HTable(configuration, tableName) ;  
        Scan scan = new Scan () ;  
        scan.addColumn("cf1".getBytes(), "column1".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column2".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column3".getBytes()) ;  
        //根据rowkey查询  
        //RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("1"));  
        //通过column查询  
        Filter filter = new SingleColumnValueFilter("cf1".getBytes(),"column1".getBytes(), CompareFilter.CompareOp.GREATER,columnValue.getBytes());  
        scan.setFilter(filter);  
        ResultScanner scanner = table.getScanner(scan) ;  
          
        for(Result result : scanner)  
        {  
            for(KeyValue keyValue : result.raw())  
            {  
                System.out.println("第 "+new String(keyValue.getRow())+" 行 ,"+new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));  
            }  
            System.out.println();  
        }  
          
    }  
      
    /** 
     * 模糊匹配rowkey 
     * @param tableName 
     * @param rowKeyRegex 
     * @throws Exception 
     */  
    public static void fuzzyQueryByRowkey(String tableName, String rowKeyRegex) throws Exception  
    {  
        HTable table = new HTable(configuration, tableName) ;  
        RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex)) ;  
//      PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());  
        Scan scan = new Scan();  
        scan.addColumn("cf1".getBytes(), "column1".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column2".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column3".getBytes()) ;  
        scan.setFilter(filter);  
          
        ResultScanner scanner = table.getScanner(scan);  
        int num=0;  
        for(Result result : scanner)  
        {  
            num ++ ;  
              
            System.out.println("rowKey:"+new String(result.getRow()));  
            for(KeyValue keyValue : result.raw())  
            {  
                System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));  
            }  
            System.out.println();  
        }  
        System.out.println(num);  
    }  
      
      
    /** 
     * 组合条件查询 
     * @param tableName 
     * @param column1 
     * @param column2 
     * @param column3 
     * @throws Exception 
     */  
    public static void multiConditionQuery(String tableName, String column1, String column2,String column3) throws Exception  
    {  
        HTable table = new HTable(configuration, tableName) ;  
        Scan scan = new Scan();  
        scan.addColumn("cf1".getBytes(), "column1".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column2".getBytes()) ;  
        scan.addColumn("cf1".getBytes(), "column3".getBytes()) ;  
          
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf1".getBytes(),"column1".getBytes(), CompareFilter.CompareOp.EQUAL,column1.getBytes());  
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf1".getBytes(),"column2".getBytes(), CompareFilter.CompareOp.EQUAL,column2.getBytes());  
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter("cf1".getBytes(),"column3".getBytes(), CompareFilter.CompareOp.EQUAL,column3.getBytes());  
          
        FilterList filterAll = new FilterList();  
        filterAll.addFilter(filter1) ;  
          
        //与sql查询的in (?,?)一样的效果  
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);  
        filterList.addFilter(filter2);  
        filterList.addFilter(filter3);  
          
        filterAll.addFilter(filterList);  
        scan.setFilter(filterAll);  
          
        ResultScanner scanner = table.getScanner(scan);  
        for(Result result : scanner)  
        {  
            System.out.println("rowKey:"+new String(result.getRow()));  
            for(KeyValue keyValue : result.raw())  
            {  
                System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));  
            }  
            System.out.println();  
        }  
          
    }  

    public static void main(String[] args) throws Exception{  
        // TODO Auto-generated method stub  
//        createTable("test_table") ;  
//        initData("test_table");  
//      deleteRow("test_table","10");  
//      List list =  new ArrayList();  
//      list.add("1");  
//      list.add("2");  
//      deleteRowKeys("test_table",list);  
          
//      get("test_table","11");  
//      scan("test_table","1","4");  
//      testSingleColumnValueFilter("test_table","11111");  
//      fuzzyQueryByRowkey("test_table","1");  
//      multiConditionQuery("test_table", "the first column", "the second column", "the third column");  
//      fuzzyQueryByRowkey("test_table", "1");  
//      fuzzyQueryBycolumn("test_table", "ee*");  
    } 

}
