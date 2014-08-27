package com.datatorrent.api;

/**
 * Interface to reading checkpoint stats of any operator.
 */
public interface CheckpointAgent
{
  /**
   * This returns the checkpoint status of the operator
   * @return
   */
  Stats.CheckpointStats getCheckpointStats();
}
