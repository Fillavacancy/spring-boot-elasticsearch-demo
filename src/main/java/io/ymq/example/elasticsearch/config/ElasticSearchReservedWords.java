package io.ymq.example.elasticsearch.config;

public enum ElasticSearchReservedWords {

    DYNAMIC("dynamic"),
    INDEX_MAPPER_DYNAMIC("index.mapper.dynamic"),
    STRICT("strict"),
    PROPERTIES("properties"),
    TYPE("type"),
    INDEX("index"),
    STORE("store"),

    ANALYZER("analyzer"),
    SEARCHANALYZER("search_analyzer"),
    NOT_ANALYZED("not_analyzed"),
    IK_MAX_WORD("ik_max_word"),
    IK_SMART("ik_smart"),
    STANDARD("standard"),
    ENGLISH("english"),

    YES("yes"),
    FORMAT("format"),

    FLOAT("float"),
    BOOLEAN("boolean"),
    TEXT("text"),
    KEYWORD("keyword"),
    DOUBLE("double"),
    DATE("date"),

    CLUSTER_NAME("cluster.name"),
    CLIENT_TRANSPORT_SNIFF("client.transport.sniff"),
    THREAD_POOL_SEARCH_SIZE("thread_pool.search.size"),
    NUMBER_OF_SHARDS("number_of_shards"),
    NUMBER_OF_REPLICAS("number_of_replicas"),
    INDEX_MAX_RESULT_WINDOW("index.max_result_window"),
    TYPE_NAME("full");

    private String text;

    public String getText() {
        return text;
    }

    private ElasticSearchReservedWords(String text) {
        this.text = text;
    }
}
