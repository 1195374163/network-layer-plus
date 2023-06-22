package pt.unl.fct.di.novasys.network.listeners;

import pt.unl.fct.di.novasys.network.Connection;

public interface InConnListener<T> {

    void inboundConnectionUp(Connection<T> con);

    void inboundConnectionDown(Connection<T> con, Throwable cause);
    
    // 在创建netty的服务端时收到来自是否创建成功的反馈Socket是否正常开启和关闭
    void serverSocketBind(boolean success, Throwable cause);

    void serverSocketClose(boolean success, Throwable cause);
}