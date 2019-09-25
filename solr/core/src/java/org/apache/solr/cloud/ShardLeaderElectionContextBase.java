/* Copyright © 2019 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws. 
CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent. */
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShardLeaderElectionContextBase extends ElectionContext {
    
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrZkClient zkClient;
  protected String shardId;
  protected String collection;
  protected LeaderElector leaderElector;
  private Integer leaderZkNodeParentVersion;

  // Prevents a race between cancelling and becoming leader.
  private final Object lock = new Object();

  public ShardLeaderElectionContextBase(LeaderElector leaderElector,
      final String shardId, final String collection, final String coreNodeName,
      ZkNodeProps props, ZkStateReader zkStateReader) {
    super(coreNodeName, ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
        + "/leader_elect/" + shardId, ZkStateReader.getShardLeadersPath(
        collection, shardId), props, zkStateReader.getZkClient());
    this.leaderElector = leaderElector;
    this.zkClient = zkStateReader.getZkClient();
    this.shardId = shardId;
    this.collection = collection;
  }
  
  @Override
  public void cancelElection() throws InterruptedException, KeeperException {
    super.cancelElection();
    synchronized (lock) {
      if (leaderZkNodeParentVersion != null) {
        try {
          // We need to be careful and make sure we *only* delete our own leader registration node.
          // We do this by using a multi and ensuring the parent znode of the leader registration node
          // matches the version we expect - there is a setData call that increments the parent's znode
          // version whenever a leader registers.
          log.debug("Removing leader registration node on cancel: {} {}", leaderPath, leaderZkNodeParentVersion);
          List<Op> ops = new ArrayList<>(2);
          ops.add(Op.check(new Path(leaderPath).getParent().toString(), leaderZkNodeParentVersion));
          ops.add(Op.delete(leaderPath, -1));
          zkClient.multi(ops, true);
        } catch (KeeperException.NoNodeException nne) {
          // no problem
          log.debug("No leader registration node found to remove: {}", leaderPath);
        } catch (KeeperException.BadVersionException bve) {
          log.info("Cannot remove leader registration node because the current registered node is not ours: {}", leaderPath);
          // no problem
        } catch (InterruptedException e) {
          throw e;
        } catch (Exception e) {
          SolrException.log(log, e);
        }
        leaderZkNodeParentVersion = null;
      } else {
        log.info("No version found for ephemeral leader parent node, won't remove previous leader registration.");
      }
    }
  }
  
  @Override
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    // register as leader - if an ephemeral is already there, wait to see if it goes away
    
    if (!zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
      log.info("Will not register as leader because collection appears to be gone.");
      return;
    }
    
    String parent = new Path(leaderPath).getParent().toString();
    ZkCmdExecutor zcmd = new ZkCmdExecutor(30000);
    // only if /collections/{collection} exists already do we succeed in creating this path
    zcmd.ensureExists(parent, (byte[])null, CreateMode.PERSISTENT, zkClient, 2);

    try {
      RetryUtil.retryOnThrowable(NodeExistsException.class, 60000, 5000, () -> {
        synchronized (lock) {
          log.debug("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);
          List<Op> ops = new ArrayList<>(2);

          // We use a multi operation to get the parent nodes version, which will
          // be used to make sure we only remove our own leader registration node.
          // The setData call used to get the parent version is also the trigger to
          // increment the version. We also do a sanity check that our leaderSeqPath exists.

          ops.add(Op.check(leaderSeqPath, -1));
          ops.add(Op.create(leaderPath, Utils.toJSON(leaderProps), zkClient.getZkACLProvider().getACLsToAdd(leaderPath), CreateMode.EPHEMERAL));
          ops.add(Op.setData(parent, null, -1));
          List<OpResult> results;

          results = zkClient.multi(ops, true);
          for (OpResult result : results) {
            if (result.getType() == ZooDefs.OpCode.setData) {
              SetDataResult dresult = (SetDataResult) result;
              Stat stat = dresult.getStat();
              leaderZkNodeParentVersion = stat.getVersion();
              return;
            }
          }
          assert leaderZkNodeParentVersion != null;
        }
      });
    } catch (Throwable t) {
      if (t instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) t;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", t);
    } 
    
    assert shardId != null;
    ZkNodeProps m = ZkNodeProps.fromKeyVals(Overseer.QUEUE_OPERATION,
        OverseerAction.LEADER.toLower(), ZkStateReader.SHARD_ID_PROP, shardId,
        ZkStateReader.COLLECTION_PROP, collection, ZkStateReader.BASE_URL_PROP,
        leaderProps.getProperties().get(ZkStateReader.BASE_URL_PROP),
        ZkStateReader.CORE_NAME_PROP,
        leaderProps.getProperties().get(ZkStateReader.CORE_NAME_PROP),
        ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
    Overseer.getStateUpdateQueue(zkClient).offer(Utils.toJSON(m));
  }

  public LeaderElector getLeaderElector() {
    return leaderElector;
  }

  Integer getLeaderZkNodeParentVersion() {
    synchronized (lock) {
      return leaderZkNodeParentVersion;
    }
  }
}