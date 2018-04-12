/**
 * FileName: Corpus
 * Author:   xiangjunzhong
 * Date:     2017/11/28 16:50
 * Description: 要保存的实体对象 语料
 */
package io.ymq.example.elasticsearch.model;

import java.util.Date;

/**
 * 〈一句话功能简述〉<br>
 * 要保存的实体对象 语料
 *
 * @author xiangjunzhong
 * @create 2017/11/28 16:50
 * @since 1.0.0
 */
public class Corpus {

    /**
     * ID
     */
    private String id;

    /**
     * 原文
     */
    private String original;

    /**
     * 译文
     */
    private String translation;

    /**
     * 创建人
     */
    private String founder;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date changeTime;

    /**
     * 备注
     */
    private String description;

    /**
     * 权重
     */
    private float boostFactor = 1.0F;

    /**
     * 是否使用分词
     */
    private Boolean participle = true;

    // 关键词
   /* private List<String> keywords = new ArrayList<String>();*/

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    public String getTranslation() {
        return translation;
    }

    public void setTranslation(String translation) {
        this.translation = translation;
    }

    public String getFounder() {
        return founder;
    }

    public void setFounder(String founder) {
        this.founder = founder;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getChangeTime() {
        return changeTime;
    }

    public void setChangeTime(Date changeTime) {
        this.changeTime = changeTime;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getParticiple() {
        return participle;
    }

    public void setParticiple(Boolean participle) {
        this.participle = participle;
    }

    public float getBoostFactor() {
        return boostFactor;
    }

    public void setBoostFactor(float boostFactor) {
        this.boostFactor = boostFactor;
    }

/*    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }*/

    @Override
    public String toString() {
        return "Corpus{" +
                "id='" + id + '\'' +
                ", original='" + original + '\'' +
                ", translation='" + translation + '\'' +
                ", founder='" + founder + '\'' +
                ", createTime=" + createTime +
                ", changeTime=" + changeTime +
                ", description='" + description + '\'' +
                ", boostFactor=" + boostFactor +
                ", participle=" + participle +
                '}';
    }
}