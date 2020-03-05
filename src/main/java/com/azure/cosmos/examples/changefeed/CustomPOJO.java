package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.JsonSerializable;

public class CustomPOJO {
    private String id;

    public CustomPOJO() {

    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
