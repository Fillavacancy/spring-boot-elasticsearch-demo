package io.ymq.example.elasticsearch.config;

public enum SearchDocumentFieldName {

    ORIGINAL("original"),
    TRANSLATION("translation"),
    FOUNDER("founder"),
    CREATETIME("createTime"),
    CHANGETIME("changeTime"),
    DESCRIPTION("description"),
    BOOSTFACTOR("boostFactor"),
    KEYWORDS("keywords");

    private String fieldName;

    private SearchDocumentFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }
}
