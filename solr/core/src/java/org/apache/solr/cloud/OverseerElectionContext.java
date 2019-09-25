/* Copyright © 2019 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws. 
CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent. */
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OverseerElectionContext extends ElectionContext {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final SolrZkClient zkClient;
    private Overseer overseer;
    public static final String OVERSEER_ELECT = "/overseer_elect";

    public OverseerElectionContext(SolrZkClient zkClient, Overseer overseer, final String zkNodeName) {
      super(zkNodeName, OVERSEER_ELECT, OVERSEER_ELECT + "/leader", null, zkClient);
      this.overseer = overseer;
      this.zkClient = zkClient;
      try {
        new ZkCmdExecutor(zkClient.getZkClientTimeout()).ensureExists(OVERSEER_ELECT, zkClient);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    @Override
    void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs) throws KeeperException,
        InterruptedException {
      log.info("I am going to be the leader {}", id);
      final String id = leaderSeqPath
          .substring(leaderSeqPath.lastIndexOf("/") + 1);
      ZkNodeProps myProps = new ZkNodeProps("id", id);

      zkClient.makePath(leaderPath, Utils.toJSON(myProps),
          CreateMode.EPHEMERAL, true);
      if(pauseBeforeStartMs >0){
        try {
          Thread.sleep(pauseBeforeStartMs);
        } catch (InterruptedException e) {
          Thread.interrupted();
          log.warn("Wait interrupted ", e);
        }
      }
      if (overseer.getZkController() == null || overseer.getZkController().getCoreContainer() == null || !overseer.getZkController().getCoreContainer().isShutDown()) {
        overseer.start(id);
      }
    }
    
    @Override
    public void cancelElection() throws InterruptedException, KeeperException {
      super.cancelElection();
      overseer.close();
    }
    
    @Override
    public void close() {
      overseer.close();
    }

    @Override
    public ElectionContext copy() {
      return new OverseerElectionContext(zkClient, overseer ,id);
    }
    
    @Override
    public void joinedElectionFired() {
      overseer.close();
    }
    
    @Override
    public void checkIfIamLeaderFired() {
      // leader changed - close the overseer
      overseer.close();
    }

  }