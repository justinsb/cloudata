package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import com.google.common.util.concurrent.ListenableFuture;

public class LoginRequest extends IscsiRequest {

    public static final int OPCODE = 0x03;

    public LoginRequest(IscsiSession session, ByteBuf buf) {
        super(session, buf);
        assert getOpcode() == OPCODE;
    }

    @Override
    public ListenableFuture<Void> start() {
        LoginResponse response = new LoginResponse();
        response.isid = getIsid();
        response.currentStage = Stage.Negotiation;
        response.nextStage = Stage.FullFeaturePhase;
        response.targetSessionIdentifier = session.getTargetSessionIdentifier();

        response.initiatorTaskTag = getInitiatorTaskTag();

        response.statSN = 1;

        int cmdSN = getCmdSN();
        response.expectedCommandSN = cmdSN + 1;
        response.maxCommandSN = cmdSN + 32;

        // TODO: Support HeaderDigest / DataDigest
        response.data.put("HeaderDigest", "None");
        response.data.put("DataDigest", "None");
        response.data.put("MaxConnections", "1");
        response.data.put("TargetAlias", "Cloudata Target");
        response.data.put("TargetPortalGroupTag", "1");
        response.data.put("InitialR2T", "Yes");
        response.data.put("ImmediateData", "Yes");
        response.data.put("MaxBurstLength", "262144");
        response.data.put("FirstBurstLength", "65536");
        response.data.put("DefaultTime2Wait", "2");
        response.data.put("DefaultTime2Retain", "0");
        response.data.put("MaxOutstandingR2T", "1");
        response.data.put("ErrorRecoveryLevel", "0");

        return sendFinal(response);
    }

}
