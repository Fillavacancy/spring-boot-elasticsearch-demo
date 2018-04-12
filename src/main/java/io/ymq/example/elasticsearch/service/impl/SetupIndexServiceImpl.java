/**
 * FileName: SetupIndexServiceImpl
 * Author:   xiangjunzhong
 * Date:     2017/11/27 13:02
 * Description: 索引设置业务逻辑层接口实现类
 */
package io.ymq.example.elasticsearch.service.impl;

import io.ymq.example.elasticsearch.config.IndexSchemaBuilder;
import io.ymq.example.elasticsearch.service.SetupIndexService;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

/**
 * 〈一句话功能简述〉<br>
 * 索引设置业务逻辑层接口实现类
 *
 * @author xiangjunzhong
 * @create 2017/11/27 13:02
 * @since 1.0.0
 */
@Service
public class SetupIndexServiceImpl implements SetupIndexService {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(SetupIndexServiceImpl.class);

    @Autowired
    private TransportClient client;

    @Autowired
    private IndexSchemaBuilder indexSchemaBuilder;

    /**
     * 更新索引设置
     *
     * @param index
     * @param settings
     */
    @Override
    public void updateIndexSettings(String index, Map<String, Object> settings) {
        // 关闭索引
        client.admin().indices().prepareClose(index).get();

        // 修改索引设置
        client.admin().indices().prepareUpdateSettings(index).setSettings(settings).get();

        // 打开索引
        client.admin().indices().prepareOpen(index).get();
    }

    /**
     * 更新文档类型映射
     *
     * @param index
     * @param documentType
     */
    @Override
    public void updateDocumentTypeMapping(String index, String documentType) {
        try {
            client.admin().indices().preparePutMapping(index).setType(documentType).setSource(indexSchemaBuilder.getDocumentTypeMapping(documentType)).get();
        } catch (IOException e) {
            throw new RuntimeException("Error occurend while generating mapping for document type", e);
        }
    }

    /**
     * 获取索引设置
     *
     * @param index
     * @param settingName
     * @return
     */
    @Override
    public String getIndexSettings(String index, String settingName) {
        String settingValue = null;

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setRoutingTable(true).setNodes(true).setIndices(index).get();

        for (IndexMetaData indexMetaData : clusterStateResponse.getState().getMetaData()) {
            settingValue = indexMetaData.getSettings().get(settingName);
        }

        return settingValue;
    }
}