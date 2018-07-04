import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;


import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @ Description:
 * @ Date: Created in 14:42 03/07/2018
 * @ Author: Anthony_Duan
 */
public class HbaseUtilTest {

    @Test
    public void createTable() {
        System.out.println("HbaseUtilTest.createTable:"
                +HbaseUtil.createTable("TestTable1", new String[]{"TestInfo_1","TestInfo_2"}));
    }

    @Test
    public void deleteTable() {
        System.out.println("HbaseUtilTest.deleteTable:"+HbaseUtil.deleteTable("TestTable"));
    }

    @Test
    public void putRow() {
        System.out.println("HbaseUtilTest.putRow:"
                +HbaseUtil.putRow("TestTable","2","TestInfo_1","name","file.txt"));
    }

    @Test
    public void putRows() {
    }

    @Test
    public void getRow() {
    }

    @Test
    public void getRow1() {
    }

    @Test
    public void getScanner() {
        ResultScanner resultScanner = HbaseUtil.getScanner("TestTable1");
        for (Result r : resultScanner) {
            System.out.println(Arrays.toString(r.getRow()));
        }
    }

    @Test
    public void getScanner1() {
    }

    @Test
    public void getScanner2() {
    }

    @Test
    public void deleteRow() {
    }

    @Test
    public void deleteQualifier() {
    }

    @Test
    public void deleteColumnFamily() {
    }
}