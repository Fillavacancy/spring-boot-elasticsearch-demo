/**
 * FileName: ElasticsearchConfig
 * Author:   xiangjunzhong
 * Date:     2017/11/28 16:47
 * Description: elasticsearch 配置
 */
package io.ymq.example.elasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * 〈一句话功能简述〉<br>
 * elasticsearch 配置
 *
 * @author xiangjunzhong
 * @create 2017/11/28 16:47
 * @since 1.0.0
 */
@Configuration
public class ElasticsearchConfig {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

    /**
     * ES 集群地址
     */
    @Value("${elasticsearch.cluster.hostName}")
    private String hostName;

    /**
     * 端口
     */
    @Value("${elasticsearch.cluster.port}")
    private Integer port;

    /**
     * 集群名称
     */
    @Value("${elasticsearch.cluster.clusterName}")
    private String clusterName;

    /**
     * 连接池
     */
    @Value("${elasticsearch.cluster.poolSize}")
    private Integer poolSize;

    /**
     * 是否开启嗅探机制
     */
    @Value("${elasticsearch.cluster.transportSniff}")
    private boolean transportSniff;

    @Bean
    public TransportClient init() {

        TransportClient transportClient = null;

        try {

            // 配置信息
            Settings esSetting = Settings.builder()
                    .put(ElasticSearchReservedWords.CLUSTER_NAME.getText(), clusterName)
                    .put(ElasticSearchReservedWords.CLIENT_TRANSPORT_SNIFF.getText(), transportSniff)// 增加嗅探机制，找到ES集群
                    .put(ElasticSearchReservedWords.THREAD_POOL_SEARCH_SIZE.getText(), poolSize)// 增加线程池个数
                    //.put("client.transport.ping_timeout", "30s")// ping一个节点的响应时间 默认5秒
                    //.put("client.transport.nodes_sampler_interval", "30s")// sample/ping 节点的时间间隔，默认是5s
                    .build();

            transportClient = new PreBuiltTransportClient(esSetting);

            // 5.5.3 版本 写法
           /* InetSocketTransportAddress inetSocketTransportAddress = new InetSocketTransportAddress(InetAddress.getByName(hostName), Integer.valueOf(port));
            transportClient.addTransportAddresses(inetSocketTransportAddress);*/

            // 6.0.0 版本 写法
            TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(hostName), port);
            transportClient.addTransportAddresses(transportAddress);
            if (transportClient.connectedNodes().size() == 0) {
                logger.error("There are no active nodes available for the transport, it will be automatically added once nodes are live!");
            }

        } catch (Exception e) {
            logger.error("elasticsearch TransportClient create error!!!", e);
        }
        return transportClient;
    }
}