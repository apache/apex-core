/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableMap;

import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;

/**
 * Write ahead log for DAG changes.
 * Operations need to be registered with the journal instance before writing.
 * Registered prototype instances will be used to apply changes on read.
 *
 * @since 0.9.2
 */
public final class Journal
{
  private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

  private enum RecoverableOperation
  {
    OPERATOR_STATE(PTOperator.SET_OPERATOR_STATE),
    CONTAINER_STATE(PTContainer.SET_CONTAINER_STATE),
    OPERATOR_PROPERTY(StreamingContainerManager.SET_OPERATOR_PROPERTY),
    PHYSICAL_OPERATOR_PROPERTY(StreamingContainerManager.SET_PHYSICAL_OPERATOR_PROPERTY);

    private static final Map<Class<? extends Recoverable>, Integer> classToId;

    static {
      final ImmutableMap.Builder<Class<? extends Recoverable>, Integer> builder = ImmutableMap.builder();
      for (RecoverableOperation recoverableOperation : RecoverableOperation.values()) {
        builder.put(recoverableOperation.operation.getClass(), recoverableOperation.ordinal());
      }
      classToId = builder.build();
    }

    private final Recoverable operation;

    RecoverableOperation(Recoverable operation)
    {
      this.operation = operation;
    }

    private static RecoverableOperation get(int id)
    {
      return (id < values().length) ? values()[id] : null;
    }

    private static Integer getId(Class<? extends Recoverable> operationClass)
    {
      return classToId.get(operationClass);
    }
  }

  public interface Recoverable
  {
    void read(Object object, Input in) throws KryoException;

    void write(Output out) throws KryoException;
  }

  private final StreamingContainerManager scm;
  private final AtomicReference<Output> output;
  private final AtomicBoolean replayMode;

  public Journal(StreamingContainerManager scm)
  {
    this.scm = scm;
    output = new AtomicReference<>();
    replayMode = new AtomicBoolean(false);
  }

  public void setOutputStream(@Nullable final OutputStream out) throws IOException
  {
    final Output output;
    if (out != null) {
      output = new Output(4096, -1)
      {
        @Override
        public void flush() throws KryoException
        {
          super.flush();
          // Kryo does not flush internal output stream during flush. We need to flush it explicitly.
          try {
            getOutputStream().flush();
          } catch (IOException e) {
            throw new KryoException(e);
          }
        }
      };
      output.setOutputStream(out);
    } else {
      output = null;
    }

    final Output oldOut = this.output.getAndSet(output);
    if (oldOut != null && oldOut.getOutputStream() != out) {
      synchronized (oldOut) {
        oldOut.close();
      }
    }
  }

  final void write(Recoverable op)
  {
    if (replayMode.get()) {
      throw new IllegalStateException("Request to write while journal is replaying operations");
    }
    Integer classId = RecoverableOperation.getId(op.getClass());
    if (classId == null) {
      throw new IllegalArgumentException("Class not registered " + op.getClass());
    }
    while (true) {
      final Output out = output.get();
      if (out != null) {
        // need to atomically write id, operation and flush the output stream
        synchronized (out) {
          try {
            LOG.debug("WAL write {}", RecoverableOperation.get(classId));
            out.writeInt(classId);
            op.write(out);
            out.flush();
            break;
          } catch (KryoException e) {
            // check that no other threads sneaked between get() and synchronized block and set output stream to a new
            // stream or null leading to the current stream being closed
            if (output.get() == out) {
              throw e;
            }
          }
        }
      } else {
        LOG.warn("Journal output stream is null. Skipping write to the WAL.");
        break;
      }
    }
  }

  final void replay(final InputStream input)
  {
    if (replayMode.compareAndSet(false, true)) {
      Input in = new Input(input);
      try {
        LOG.debug("Start replaying WAL");
        while (!in.eof()) {
          final int opId = in.readInt();
          final RecoverableOperation recoverableOperation = RecoverableOperation.get(opId);
          if (recoverableOperation == null) {
            throw new IllegalArgumentException("No reader registered for id " + opId);
          }
          LOG.debug("Replaying {}", recoverableOperation);
          switch (recoverableOperation) {
            case OPERATOR_STATE:
            case CONTAINER_STATE:
              recoverableOperation.operation.read(scm.getPhysicalPlan(), in);
              break;
            case OPERATOR_PROPERTY:
            case PHYSICAL_OPERATOR_PROPERTY:
              recoverableOperation.operation.read(scm, in);
              break;
            default:
              throw new IllegalArgumentException("Unsupported recoverable operation " + recoverableOperation);
          }
        }
      } finally {
        LOG.debug("Done replaying WAL");
        replayMode.set(false);
      }
    } else {
      throw new IllegalStateException("Request to replay while journal is already replaying other operations");
    }
  }

}
