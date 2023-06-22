package pt.unl.fct.di.novasys.network.pipeline;

import io.netty.channel.*;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.ReadTimeoutException;
import pt.unl.fct.di.novasys.network.Connection;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.messaging.NetworkMessage;
import pt.unl.fct.di.novasys.network.messaging.control.HeartbeatMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ConnectionHandler<T> extends ChannelDuplexHandler implements Connection<T> {

    private static final Logger logger = LogManager.getLogger(ConnectionHandler.class);
    
    // 连接的对象
    Host peer;
    Attributes peerAttributes;
    Attributes selfAttributes;
    
    // 消息的编码和解码器
    MessageEncoder<T> encoder;
    MessageDecoder<T> decoder;

    // 如果是out 是serverChannel，如果是in ，是Channel
    Channel channel;
    //事件执行器：在
    EventLoop loop;
    
    
    // 从连接中拿出消息：转发到上层调用者
    private final MessageListener<T> consumer;
    
    
    //代表此连接是接入的还是接出的连接
    private final boolean incoming;

    
    public ConnectionHandler(MessageListener<T> consumer, EventLoop loop, boolean incoming, Attributes selfAttrs) {
        this.consumer = consumer;
        this.incoming = incoming;
        this.selfAttributes = selfAttrs;
        this.loop = loop;
    }

    
    
    
    
    
    
    
    // TCP通道从通道中读，将读到的app信息发送给上层，若是控制信息忽略----是入连接读
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        NetworkMessage netMsg = (NetworkMessage) msg;
        if (netMsg.code == NetworkMessage.CTRL_MSG) return;
        consumer.deliverMessage((T) netMsg.payload, this);
    }
    
    
    //给指定通道写  一般是写通道
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        ctx.write(msg, promise.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                exceptionCaught(ctx, future.cause());
            }
        }));
    }
    
    
    //事件被触发
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.WRITER_IDLE) {
                ctx.channel().writeAndFlush(new NetworkMessage(NetworkMessage.CTRL_MSG, new HeartbeatMessage()));
            } else if (e.state() == IdleState.READER_IDLE) {
                ctx.pipeline().fireExceptionCaught(ReadTimeoutException.INSTANCE);
            }
        } else if (!(evt instanceof ChannelInputShutdownReadComplete)) {
            internalUserEventTriggered(ctx, evt);
        }
    }
    
    
    // 自定义的事件触发
    abstract void internalUserEventTriggered(ChannelHandlerContext ctx, Object evt);
    
    
  
    
    
    
    
    //---------------- 下面是Connection接口中的方法
    
    
    public final Host getPeer() {
        return peer;
    }
    
    public final Attributes getPeerAttributes() {
        return peerAttributes;
    }

    public final Attributes getSelfAttributes() {
        return selfAttributes;
    }
    
    public boolean isInbound() {
        return incoming;
    }

    public boolean isOutbound() {
        return !incoming;
    }
    
    @Override
    public EventLoop getLoop() {
        return loop;
    }
    
    
    
    
    
    
    @Override
    public long getSentAppBytes() {
        return encoder.getSentAppBytes();
    }

    @Override
    public long getSentAppMessages() {
        return encoder.getSentAppMessages();
    }

    @Override
    public long getSentControlBytes() {
        return encoder.getSentControlBytes();
    }

    @Override
    public long getSentControlMessages() {
        return encoder.getSentControlMessages();
    }

    @Override
    public long getReceivedAppBytes() {
        return decoder.getReceivedAppBytes();
    }

    @Override
    public long getReceivedAppMessages() {
        return decoder.getReceivedAppMessages();
    }

    @Override
    public long getReceivedControlBytes() {
        return decoder.getReceivedControlBytes();
    }

    @Override
    public long getReceivedControlMessages() {
        return decoder.getReceivedControlMessages();
    }

}
