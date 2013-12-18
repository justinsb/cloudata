package com.cloudata.structured.sql;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;

import org.joda.time.DateTime;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

public class FakeRemoteTaskFactory implements RemoteTaskFactory {
    private final Executor executor;

    public FakeRemoteTaskFactory(Executor executor) {
        this.executor = executor;
    }

    @Override
    public RemoteTask createRemoteTask(Session session, TaskId taskId, Node node, PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits, Map<PlanNodeId, OutputReceiver> outputReceivers,
            OutputBuffers outputBuffers) {
        return new MockRemoteTask(taskId, fragment, executor);
    }

    private static class MockRemoteTask implements RemoteTask {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

        private final URI location;
        private final TaskStateMachine taskStateMachine;
        private final TaskContext taskContext;
        private final SharedBuffer sharedBuffer;

        private final PlanFragment fragment;

        @GuardedBy("this")
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

        @GuardedBy("this")
        private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

        public MockRemoteTask(TaskId taskId, PlanFragment fragment, Executor executor) {
            this.taskStateMachine = new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotNull(executor,
                    "executor is null"));

            Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
            this.taskContext = new TaskContext(taskStateMachine, executor, session, new DataSize(256, MEGABYTE),
                    new DataSize(1, MEGABYTE), true);

            this.location = URI.create("fake://task/" + taskId);

            this.sharedBuffer = new SharedBuffer(checkNotNull(new DataSize(1, Unit.BYTE), "maxBufferSize is null"),
                    INITIAL_EMPTY_OUTPUT_BUFFERS);
            this.fragment = checkNotNull(fragment, "fragment is null");
        }

        @Override
        public String getNodeId() {
            return "node";
        }

        @Override
        public TaskInfo getTaskInfo() {
            TaskState state = taskStateMachine.getState();
            List<FailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(taskStateMachine.getTaskId(), nextTaskInfoVersion.getAndIncrement(), state, location,
                    DateTime.now(), sharedBuffer.getInfo(), ImmutableSet.<PlanNodeId> of(), taskContext.getTaskStats(),
                    failures, taskContext.getOutputItems());
        }

        @Override
        public void start() {
        }

        @Override
        public void addSplit(PlanNodeId sourceId, Split split) {
            checkNotNull(split, "split is null");
            splits.put(sourceId, split);
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId) {
            noMoreSplits.add(sourceId);
            if (noMoreSplits.containsAll(fragment.getSources())) {
                taskStateMachine.finished();
            }
        }

        @Override
        public void setOutputBuffers(OutputBuffers outputBuffers) {
            sharedBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public void addStateChangeListener(final StateChangeListener<TaskInfo> stateChangeListener) {
            taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>() {
                @Override
                public void stateChanged(TaskState newValue) {
                    stateChangeListener.stateChanged(getTaskInfo());
                }
            });
        }

        @Override
        public void cancel() {
            taskStateMachine.cancel();
        }

        @Override
        public Duration waitForTaskToFinish(Duration maxWait) throws InterruptedException {
            while (true) {
                TaskState currentState = taskStateMachine.getState();
                if (maxWait.toMillis() <= 1 || currentState.isDone()) {
                    return maxWait;
                }
                maxWait = taskStateMachine.waitForStateChange(currentState, maxWait);
            }
        }

        @Override
        public int getQueuedSplits() {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return splits.size();
        }
    }
}