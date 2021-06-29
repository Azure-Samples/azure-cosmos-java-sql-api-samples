// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

public class UserSession {

    private String id;
    private String tenantId;
    private String userId;
    private String sessionId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTenantId(){return this.tenantId;}

    public void setTenantId(String tenantId){this.tenantId = tenantId;}

    public String getUserId(){return this.userId;}

    public void setUserId(String userId){this.userId = userId;}

    public String getSessionId(){return this.sessionId;}

    public void setSessionId(String sessionId){this.sessionId = sessionId;}



}
