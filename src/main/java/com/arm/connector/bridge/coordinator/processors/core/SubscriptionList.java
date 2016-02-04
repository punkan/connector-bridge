/**
 * @file    SubscriptionList.java
 * @brief   mDS subscription list manager
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Use of this software is restricted by the terms of the license under which this software has been
 * distributed (the "License"). Any use outside the express terms of the License, including wholly
 * unlicensed use, is prohibited. You may not use this software unless you have been expressly granted
 * the right to use it under the License.
 * 
 */

package com.arm.connector.bridge.coordinator.processors.core;

import com.arm.connector.bridge.core.BaseClass;
import com.arm.connector.bridge.core.ErrorLogger;
import com.arm.connector.bridge.preferences.PreferenceManager;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * mDS subscription list manager
 * @author Doug Anson
 */
public class SubscriptionList extends BaseClass {
    private ArrayList<HashMap<String,String>> m_subscriptions = null;
    private String m_non_domain = null;
    
    // constructor
    public SubscriptionList(ErrorLogger error_logger, PreferenceManager preference_manager) {
        super(error_logger,preference_manager);
        this.m_subscriptions = new ArrayList<>();
        this.m_non_domain = this.preferences().valueOf("mds_def_domain");
    }
    
    // add subscription
    public void addSubscription(String domain,String endpoint,String uri) {
        domain = this.checkAndDefaultDomain(domain);
        if (!this.containsSubscription(domain,endpoint,uri)) {
            this.errorLogger().info("Adding Subscription: " + domain + ":" + endpoint + ":" + uri);
            this.m_subscriptions.add(this.makeSubscription(domain, endpoint, uri));
        }
    }
    
    // contains a given subscription?
    public boolean containsSubscription(String domain,String endpoint,String uri) {
        boolean has_subscription = false;
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String,String> subscription = this.makeSubscription(domain, endpoint, uri);
        if (this.containsSubscription(subscription) >= 0) {
            has_subscription = true;
        }
        
        return has_subscription;
    }
    
    // remove a subscription
    public void removeSubscription(String domain,String endpoint,String uri) {
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String,String> subscription = this.makeSubscription(domain, endpoint, uri);
        int index = this.containsSubscription(subscription);
        if (index >= 0) {
            this.errorLogger().info("Removing Subscription: " + domain + ":" + endpoint + ":" + uri);
            this.m_subscriptions.remove(index);
        }
    }
    
    // contains a given subscription?
    private int containsSubscription(HashMap<String,String> subscription) {
        int index = -1;
        
        for(int i=0;i<this.m_subscriptions.size() && index < 0;++i) {
            if (this.sameSubscription(subscription,this.m_subscriptions.get(i))) {
                index = i;
            }
        }
        
        return index;
    }
    
    // compare subscriptions
    private boolean sameSubscription(HashMap<String,String> s1,HashMap<String,String> s2) {
        boolean same_subscription = false;
        
        // compare contents...
        if (s1.get("domain") != null && s2.get("domain") != null && s1.get("domain").equalsIgnoreCase(s2.get("domain"))) {
            if (s1.get("endpoint") != null && s2.get("endpoint") != null && s1.get("endpoint").equalsIgnoreCase(s2.get("endpoint"))) {
                if (s1.get("uri") != null && s2.get("uri") != null && s1.get("uri").equalsIgnoreCase(s2.get("uri"))) {
                    // they are the same
                    same_subscription = true;
                }
            }
        }
        
        return same_subscription;
    }
    
    // make subscription entry 
    private HashMap<String,String> makeSubscription(String domain,String endpoint,String uri) {
        domain = this.checkAndDefaultDomain(domain);
        HashMap<String,String> subscription = new HashMap<>();
        subscription.put("domain", domain);
        subscription.put("endpoint",endpoint);
        subscription.put("uri",uri);
        return subscription;
    }
    
    // default domain
    private String checkAndDefaultDomain(String domain) {
        if (domain == null || domain.length() <= 0) return this.m_non_domain;
        return domain;
    }
}
