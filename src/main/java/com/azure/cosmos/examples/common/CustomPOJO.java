// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

public class CustomPOJO {
    private String id;
    private String city;

    public CustomPOJO() {

    }

    public CustomPOJO(String id, String city) {
        this.id = id;
        this.city = city;
    }

    public CustomPOJO(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCity() { return city; }

    public void setCity(String city) { this.city = city; }
}
