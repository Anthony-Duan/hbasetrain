import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * @ Description:
 * @ Date: Created in 15:12 04/07/2018
 * @ Author: Anthony_Duan
 */
public class HBaseFilterTest {

    @Test
    public void createTable(){
        HbaseUtil.createTable("FileTable",new String []{"fileInfo","saveInfo"});
    }


    @Test
    public void addFileDetails() {
        HbaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "name", "file1.txt");
        HbaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "type", "txt");
        HbaseUtil.putRow("FileTable", "rowkey1", "fileInfo", "size", "1024");
        HbaseUtil.putRow("FileTable", "rowkey1", "saveInfo", "creator", "jixin");
        HbaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "name", "file2.jpg");
        HbaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "type", "jpg");
        HbaseUtil.putRow("FileTable", "rowkey2", "fileInfo", "size", "1024");
        HbaseUtil.putRow("FileTable", "rowkey2", "saveInfo", "creator", "jixin");
        HbaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "name", "file3.jpg");
        HbaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "type", "jpg");
        HbaseUtil.putRow("FileTable", "rowkey3", "fileInfo", "size", "1024");
        HbaseUtil.putRow("FileTable", "rowkey3", "saveInfo", "creator", "jixin");

    }


    @Test
    public void rowFilterTest(){

        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator("rowkey1".getBytes()));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(filter));

        ResultScanner scanner =
                HbaseUtil.getScanner("FileTable","rowkey1","rowkey3",filterList);

        if (scanner != null){
            scanner.forEach(result -> {
                System.out.println("rowkey="+ Bytes.toString(result.getRow())); //注意这里不能携程result.getRow().toString()这里打印出来的会是一个地址
                System.out.println("fileName=" +
                        Bytes.toString(result.getValue(toBytes("fileInfo"),"name".getBytes())));//Bytes.toBytes() 可以替换为.getBytes()
            });
            scanner.close();  //关闭资源 Closes the scanner and releases any resources it has allocated
        }

    }

    /**
     * 筛选出具有特定前缀行键的数据
     */
    @Test
    public void prefixFilterTest(){

        Filter filter = new PrefixFilter(toBytes("rowkey2"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner resultScanner =
                HbaseUtil.getScanner("FileTable", "rowkey1","rowkey3",filterList);
        if (resultScanner != null){
            resultScanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes.toString(result.getValue(toBytes("fileInfo"), Bytes.toBytes("name"))));
            });
            resultScanner.close();
        }
    }

    /**
     *
     * 只返回每行的行键，值为空
     */
    @Test
    public void keyOnlyFilterTest(){

        Filter filter = new KeyOnlyFilter(true);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(filter));

        ResultScanner resultScanner= HbaseUtil.getScanner("FileTable", "rowkey1", "rowkey3", filterList);

        if (resultScanner != null) {
            resultScanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes
                        .toString(result.getValue(toBytes("fileInfo"), toBytes("name"))));
            });

            resultScanner.close();
        }
    }

    /**
     * 按照列名前缀筛选单元格
     */
    @Test
    public void columnPrefixFilterTest() {
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HbaseUtil
                .getScanner("FileTable", "rowkey1", "rowkey3", filterList);

        if (scanner != null) {
            scanner.forEach(result -> {
                System.out.println("rowkey=" + Bytes.toString(result.getRow()));
                System.out.println("fileName=" + Bytes
                        .toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("name"))));
                System.out.println("fileType=" + Bytes
                        .toString(result.getValue(Bytes.toBytes("fileInfo"), Bytes.toBytes("type"))));
            });
            scanner.close();
        }
    }



}
