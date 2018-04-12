/**
 * FileName: SetupIndexService
 * Author:   xiangjunzhong
 * Date:     2017/11/27 13:00
 * Description: 索引设置业务逻辑层接口
 */
package io.ymq.example.elasticsearch.service;

import java.util.Map;

/**
 * 〈一句话功能简述〉<br>
 * 索引设置业务逻辑层接口
 *
 * @author xiangjunzhong
 * @create 2017/11/27 13:00
 * @since 1.0.0
 */
public interface SetupIndexService {

    /**
     * 更新索引设置
     *
     * @param index
     * @param settings
     */
    void updateIndexSettings(String index, Map<String, Object> settings);

    /**
     * 更新文档类型映射
     *
     * @param index
     * @param documentType
     */
    void updateDocumentTypeMapping(String index, String documentType);

    /**
     * 获取索引设置
     *
     * @param index
     * @param settingName
     * @return
     */
    String getIndexSettings(String index, String settingName);
}