// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

public class CustomPOJO2 {
    private String id;
    private String pk;

    public CustomPOJO2() {

    }

    public CustomPOJO2(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPk() { return pk; }

    public void setPk(String pk) {
        this.pk = pk;
    }

}
