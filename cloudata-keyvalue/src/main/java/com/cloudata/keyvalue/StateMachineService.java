package com.cloudata.keyvalue;

import org.robotninjas.barge.RaftService;

import com.google.common.util.concurrent.AbstractService;

public class StateMachineService extends AbstractService {
    private final KeyValueStateMachine stateMachine;
    private final RaftService raft;

    public StateMachineService(KeyValueStateMachine stateMachine, RaftService raft) {
        this.stateMachine = stateMachine;
        this.raft = raft;
    }

    @Override
    protected void doStart() {
        try {
            stateMachine.init(raft);
            this.notifyStarted();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            if (stateMachine != null) {
                stateMachine.close();
            }
            this.notifyStopped();
        } catch (Exception e) {
            this.notifyFailed(e);
        }
    }
}
