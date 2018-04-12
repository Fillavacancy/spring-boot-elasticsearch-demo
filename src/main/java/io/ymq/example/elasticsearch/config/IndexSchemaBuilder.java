/**
 * FileName: IndexSchemaBuilder
 * Author:   xiangjunzhong
 * Date:     2017/11/28 16:49
 * Description: 索引架构生成器
 */
package io.ymq.example.elasticsearch.config;

import io.ymq.example.elasticsearch.utils.ElasticsearchUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 〈一句话功能简述〉<br>
 * 索引架构生成器
 *
 * @author xiangjunzhong
 * @create 2017/11/28 16:49
 * @since 1.0.0
 */
@Component
public class IndexSchemaBuilder {

    /**
     * logger
     */
    private static final Logger logger = LoggerFactory.getLogger(IndexSchemaBuilder.class);

    /**
     * 索引动态映射
     */
    @Value("${elasticsearch.index.indexMapperDynamic}")
    private boolean indexMapperDynamic;

    /**
     * 索引最大结果窗口值
     */
    @Value("${elasticsearch.index.indexMaxResultWindowValue}")
    private Integer indexMaxResultWindowValue;

    /**
     * 主节点数量
     */
    @Value("${elasticsearch.index.numberOfShards}")
    private Integer numberOfShards;

    /**
     * 备份节点数量
     */
    @Value("${elasticsearch.index.numberOfReplicas}")
    private Integer numberOfReplicas;

    /**
     * 获取索引设置
     *
     * @param index 索引名称
     * @return
     * @throws IOException
     */
    public Settings getSettingForIndex(String index) throws IOException {
        logger.debug("Generating settings for index: {}", index);

        Settings settings = Settings.builder().loadFromSource(jsonBuilder()
                .startObject()

                // 禁用动态映射添加，将其设置为 false
                .field(ElasticSearchReservedWords.INDEX_MAPPER_DYNAMIC.getText(), indexMapperDynamic)
                .field(ElasticSearchReservedWords.INDEX_MAX_RESULT_WINDOW.getText(), indexMaxResultWindowValue)
                .field(ElasticSearchReservedWords.NUMBER_OF_SHARDS.getText(), numberOfShards)
                .field(ElasticSearchReservedWords.NUMBER_OF_REPLICAS.getText(), numberOfReplicas)
                .endObject().string(), XContentType.JSON).build();

        logger.debug("Generated settings for index {} is: {}", new Object[]{index, settings.getAsMap()});

        return settings;
    }


    /**
     * 获取文档类型映射
     *
     * @param documentType 文档类型
     * @return
     * @throws IOException
     */
    public XContentBuilder getDocumentTypeMapping(String documentType) throws IOException {
        XContentBuilder builder = jsonBuilder().prettyPrint().startObject().startObject(documentType);

        /*
            动态映射
            你可以通过 dynamic 设置来控制这些行为，它接受下面几个选项：
            true：自动添加字段（默认）
            false：忽略字段
            strict：当遇到未知字段时抛出异常
            你可以将 dynamic 默认设置为 strict
         */

        // 禁用字段动态映射
        builder.field(ElasticSearchReservedWords.DYNAMIC.getText(), ElasticSearchReservedWords.STRICT.getText());

        builder.startObject(ElasticSearchReservedWords.PROPERTIES.getText());

        addCorpusOriginalFieldMapping(builder);
        addCorpusTranslationFieldMapping(builder);
        addCorpusFounderFieldMapping(builder);
        addCorpusCreateTimeFieldMapping(builder);
        addCorpusChangeTimeFieldMapping(builder);
        addCorpusDescriptionFieldMapping(builder);
        addCorpusBoostFactorFieldMapping(builder);

        // 端性能
        builder.endObject().endObject().endObject();

        logger.debug("Generated mapping for document type {} is: {}", new Object[]{builder.prettyPrint().string()});
        return builder;
    }

    /*
        // 不使用分词器
           field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)

        // IK 分词器  Analyzer: ik_smart , ik_max_word , Tokenizer: ik_smart , ik_max_word
           field(ElasticSearchReservedWords.ANALYZER.getText(), "ik_max_word")
        // ik_max_word: 会将文本做最细粒度的拆分
        // ik_smart: 会做最粗粒度的拆分

        // 使用Elasticsearch 5.5.3 自带分词器
           field(ElasticSearchReservedWords.ANALYZER.getText(), "standard")
        ANALYZER  standard （标准分词器）、english （英文分词）和 chinese （中文分词）

        // 框架使用自身默认的分词器
            field(ElasticSearchReservedWords.INDEX.getText(), Boolean.TRUE)

       为了避免这些问题，该string领域已经分为两种新的类型：text应该用于全文搜索，并且keyword应该用于关键字搜索。
    */


    /**
     * 原文
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusOriginalFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.ORIGINAL.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.TEXT.getText())
                .field(ElasticSearchReservedWords.ANALYZER.getText(), ElasticSearchReservedWords.IK_MAX_WORD.getText())
                .field(ElasticSearchReservedWords.SEARCHANALYZER.getText(), ElasticSearchReservedWords.IK_MAX_WORD.getText())
                // 5.5.3 版本 写法
                /*.field(ElasticSearchReservedWords.STORE.getText(), ElasticSearchReservedWords.YES.getText())*/
                // 6.0.0 版本 写法
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }

    /**
     * 译文
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusTranslationFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.TRANSLATION.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.TEXT.getText())
                .field(ElasticSearchReservedWords.ANALYZER.getText(), ElasticSearchReservedWords.IK_MAX_WORD.getText())
                .field(ElasticSearchReservedWords.SEARCHANALYZER.getText(), ElasticSearchReservedWords.IK_MAX_WORD.getText())
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                // 如果要对分词的field执行聚合操作，必须将fielddata设置为true
                //.field("fielddata", Boolean.TRUE)
                .endObject();
    }

    /**
     * 创建人
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusFounderFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.FOUNDER.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.KEYWORD.getText())
                // 5.5.3 版本 写法
                // .field(ElasticSearchReservedWords.INDEX.getText(), ElasticSearchReservedWords.NOT_ANALYZED.getText())
                // 6.0.0 版本 写法
                // 不管是 index 是 true 还是 false 都不使用分词器 但是会使用默认分词器 standard
                .field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }

    /**
     * 创建时间
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusCreateTimeFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.CREATETIME.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.DATE.getText())
                .field(ElasticSearchReservedWords.FORMAT.getText(), ElasticsearchUtils.SEARCH_DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SSSZZ)
                .field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }

    /**
     * 修改时间
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusChangeTimeFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.CHANGETIME.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.DATE.getText())
                .field(ElasticSearchReservedWords.FORMAT.getText(), ElasticsearchUtils.SEARCH_DATE_FORMAT_YYYY_MM_DD_T_HH_MM_SSSZZ)
                .field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }

    /**
     * 备注
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusDescriptionFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.DESCRIPTION.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.KEYWORD.getText())
                .field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }

    /**
     * 权重 提升因素
     *
     * @param builder
     * @throws IOException
     */
    private void addCorpusBoostFactorFieldMapping(XContentBuilder builder) throws IOException {
        builder.startObject(SearchDocumentFieldName.BOOSTFACTOR.getFieldName())
                .field(ElasticSearchReservedWords.TYPE.getText(), ElasticSearchReservedWords.FLOAT.getText())
                .field(ElasticSearchReservedWords.INDEX.getText(), Boolean.FALSE)
                .field(ElasticSearchReservedWords.STORE.getText(), Boolean.TRUE)
                .endObject();
    }
}