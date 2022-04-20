package com.tabwu.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @PROJECT_NAME: canal-redis
 * @USER: tabwu
 * @DATE: 2022/4/20 14:13
 * @DESCRIPTION:
 */
public class CanalTemplate {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1. 获取连接
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("123.57.193.24", 11111), "example", "", "");

        while (true) {
            //2. 连接
            connector.connect();
            //3. 订阅数据库中binary日志
            connector.subscribe("test.*");
//            connector.subscribe("*");
            //4. 获取指定数量数据
            Message message = connector.getWithoutAck(100);
            //5. 获取entry集合
            List<CanalEntry.Entry> entryList = message.getEntries();
            //6. 判断集合是否为空,如果为空,则等待一会继续拉取数据
            if (entryList.size() <= 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //7. 遍历entryList，单条解析
                for (CanalEntry.Entry entry : entryList) {
                    //8. 获取表名
                    String tableName = entry.getHeader().getTableName();
                    //9. 获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //10. 获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();
                    //11. 判断当前entryType类型是否为ROWDATA
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //12. 反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //13. 获取操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //14. 获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            //15. 获取操作前数据
                            JSONObject beforeObj = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeObj.put(column.getName(),column.getValue());
                            }
                            //16. 获取操作后数据
                            JSONObject afterObj = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterObj.put(column.getName(),column.getValue());
                            }

                            System.out.println("table表明：" + tableName +
                                    "--------事件类型：" + eventType + "------before" + beforeObj +
                                    "---------after" + afterObj);
                        }
                    }
                }
            }
        }

    }
}
