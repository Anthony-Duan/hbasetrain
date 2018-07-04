import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;

import java.io.IOException;

/**
 * @ Description:
 * @ Date: Created in 09:25 03/07/2018
 * @ Author: Anthony_Duan
 */
public class HbaseConnTest {

    @Test
    public void getConnTest() {
        Connection conn = HbaseConn.getHBaseConn();
        System.out.println(conn.isClosed());
        HbaseConn.closeConn();
        System.out.println(conn.isClosed());
    }


    @Test
    public void getTable() {
        try {
            Table table = HbaseConn.getTable("FileTable");
            System.out.println(table);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
