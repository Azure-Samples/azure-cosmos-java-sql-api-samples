package com.azure.cosmos.examples.common;

public class CustomPOJO {
    private String id;

    public CustomPOJO() {

    }

    public CustomPOJO(String id) {
        this.id=id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
