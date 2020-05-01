package com.azure.cosmos.examples.common;

public class SimpleFamilyMemberPOJO {
    private String lastName;
    private String id;
    private Integer ttl;

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    public Integer getTtl() {
        return ttl;
    }
}
