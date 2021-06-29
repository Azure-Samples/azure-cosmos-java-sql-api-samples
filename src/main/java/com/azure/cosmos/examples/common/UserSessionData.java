// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class UserSessionData {
    public static final List<String> TenantList = Arrays.asList(new String[]{"Microsoft", "Bob's Burgers", "Oracle"});
    public static List<UserSession> buildSampleSessionData()
    {
        List<UserSession> userSessionList = new ArrayList<UserSession>();
        for (int i = 0; i < 20; i++) {
            for(int j=0;j < 10;j++) {
                UserSession temp = new UserSession();

                temp.setTenantId(TenantList.get(i % 3));
                temp.setUserId(String.valueOf(i));
                temp.setSessionId(String.valueOf((i + 1) * 100 + j));
                temp.setId(UUID.randomUUID().toString());

                userSessionList.add(temp);
            }
        }
        return userSessionList;
    }
}
