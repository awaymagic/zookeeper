package com.tuling.zk_demo.curator;


public class CuratorClusterBase extends CuratorStandaloneBase {

    // private final static  String CLUSTER_CONNECT_STR="192.168.65.163:2181,192.168.65.184:2181,192.168.65.186:2181";
    private final static  String CLUSTER_CONNECT_STR="localhost:2181";

    public String getConnectStr() {
        return CLUSTER_CONNECT_STR;
    }

}
