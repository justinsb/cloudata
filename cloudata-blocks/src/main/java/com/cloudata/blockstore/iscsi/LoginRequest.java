package com.cloudata.blockstore.iscsi;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;

public class LoginRequest extends IscsiRequest {

    public static final int OPCODE = 0x03;

    final ParameterData parameters;

    public LoginRequest(IscsiSession session, ByteBuf buf) throws IOException {
        super(session, buf);
        assert getOpcode() == OPCODE;

        this.parameters = ParameterData.read(this.getData());
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

        // Example request parameters from QEMU:
        // TargetName=<IQN>
        // DataDigest=None
        // IFMarker=No
        // ImmediateData=Yes
        // InitialR2T=No
        // ErrorRecoveryLevel=0
        // DataPDUInOrder=Yes
        // MaxBurstLength=262144
        // InitiatorName=iqn.2008-11.org.linux-kvm
        // OFMarker=No
        // SessionType=Normal
        // DataSequenceInOrder=Yes
        // HeaderDigest=None,CRC32C
        // MaxOutstandingR2T=1
        // DefaultTime2Wait=2
        // FirstBurstLength=262144
        // MaxRecvDataSegmentLength=262144
        // MaxConnections=1
        // DefaultTime2Retain=0
        // InitiatorAlias=

        // TODO: Not fakes
        // TODO: Support HeaderDigest / DataDigest
        response.data.put("HeaderDigest", "None");
        response.data.put("DataDigest", "None");
        response.data.put("TargetAlias", "Cloudata Target");
        response.data.put("MaxConnections", "1");
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

    @Override
    public String toString() {
        return "LoginRequest [parameters=" + parameters + "]";
    }

}
