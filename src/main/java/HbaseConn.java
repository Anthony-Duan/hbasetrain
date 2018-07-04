import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @ Description:
 * @ Date: Created in 08:34 03/07/2018
 * @ Author: Anthony_Duan
 */
public class HbaseConn {

    private static final HbaseConn INSTANCE = new HbaseConn();
    private static Configuration configuration ;
    private static Connection connection;

    private HbaseConn(){
        try{
            if (configuration ==null){
                configuration = HBaseConfiguration.create();
                //设置zookeeper的地址  连接HBASE只需要知道Zookeeper的地址即可
                configuration.set("hbase.zookeeper.quorum","localhost:2181");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取链接
     * @return
     */
    private Connection getConnection(){
        if (connection ==null||connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHBaseConn(){
        return INSTANCE.getConnection();
    }

    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    public static void closeConn(){
        if (connection!=null){
            try{
                connection.close();
            }catch (IOException ioe){
                ioe.printStackTrace();
            }
        }
    }

}
