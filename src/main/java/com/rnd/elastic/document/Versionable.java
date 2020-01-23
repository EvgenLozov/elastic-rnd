package com.rnd.elastic.document;

public interface Versionable {
    String getId();
    long getSeqNum();
    long getPrimaryTerm();
    void setSeqNum(long seqNum);
    void setPrimaryTerm(long primaryTerm);
}
