/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.doris.client;

import org.apache.ranger.plugin.util.TimedEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class DorisConnectionManager {
  private static final Logger LOG = LoggerFactory.getLogger(DorisConnectionManager.class);

  protected ConcurrentMap<String, org.apache.ranger.services.doris.client.DorisClient> dorisConnectionCache;
  protected ConcurrentMap<String, Boolean> repoConnectStatusMap;

  public DorisConnectionManager() {
    dorisConnectionCache = new ConcurrentHashMap<>();
    repoConnectStatusMap = new ConcurrentHashMap<>();
  }

  public org.apache.ranger.services.doris.client.DorisClient getDorisConnection(final String serviceName, final String serviceType, final Map<String, String> configs) {
    org.apache.ranger.services.doris.client.DorisClient dorisClient = null;

    if (serviceType != null) {
      dorisClient = dorisConnectionCache.get(serviceName);
      if (dorisClient == null) {
        if (configs != null) {
          final Callable<org.apache.ranger.services.doris.client.DorisClient> connectDoris = new Callable<org.apache.ranger.services.doris.client.DorisClient>() {
            @Override
            public org.apache.ranger.services.doris.client.DorisClient call() throws Exception {
              return new org.apache.ranger.services.doris.client.DorisClient(serviceName, configs);
            }
          };
          try {
            dorisClient = TimedEventUtil.timedTask(connectDoris, 10, TimeUnit.SECONDS);
          } catch (Exception e) {
            LOG.error("Error connecting to Doris cluster: " +
            serviceName + " using config: " + configs, e);
          }

          org.apache.ranger.services.doris.client.DorisClient oldClient = null;
          if (dorisClient != null) {
            oldClient = dorisConnectionCache.putIfAbsent(serviceName, dorisClient);
          } else {
            oldClient = dorisConnectionCache.get(serviceName);
          }

          if (oldClient != null) {
            if (dorisClient != null) {
              dorisClient.close();
            }
            dorisClient = oldClient;
          }
          repoConnectStatusMap.put(serviceName, true);
        } else {
          LOG.error("Connection Config not defined for asset :"
            + serviceName, new Throwable());
        }
      } else {
        try {
          dorisClient.getCatalogList("*", null);
        } catch (Exception e) {
          dorisConnectionCache.remove(serviceName);
          dorisClient.close();
          dorisClient = getDorisConnection(serviceName, serviceType, configs);
        }
      }
    } else {
      LOG.error("Asset not found with name " + serviceName, new Throwable());
    }
    return dorisClient;
  }
}
