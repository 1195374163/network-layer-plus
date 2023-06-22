package pt.unl.fct.di.novasys.network;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.network.listeners.MessageListener;
import pt.unl.fct.di.novasys.network.listeners.InConnListener;
import pt.unl.fct.di.novasys.network.listeners.OutConnListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.pipeline.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *关于netty网络层的封装
 */
public class NetworkManager<T> {

    private static final Logger logger = LogManager.getLogger(NetworkManager.class);
    
    // netty的客户端
    private final Bootstrap clientBootstrap;
    
    
    // 在TCP通道传递过来的一个事件组，从Babel层传递过来的
    private final EventLoopGroup workerGroup;
    

    // serverChannel监听其他主机到这台主机的连接
    private Channel serverChannel;

    
    // 对message的序列化和反序列化器
    private final ISerializer<T> serializer;
    
    //  TCP通道，主要是deliverMessage
    private final MessageListener<T> consumer;
    
    
    private final int hbInterval;
    private final int hbTolerance;
    
    
    
    
    static final Class<? extends Channel> channelClass;
    static final Class<? extends ServerChannel> serverChannelClass;

    static {
        if (Epoll.isAvailable()) {
            channelClass = EpollSocketChannel.class;
            serverChannelClass = EpollServerSocketChannel.class;
        } else if (KQueue.isAvailable()) {
            channelClass = KQueueSocketChannel.class;
            serverChannelClass = KQueueServerSocketChannel.class;
        } else {
            channelClass = NioSocketChannel.class;
            serverChannelClass = NioServerSocketChannel.class;
        }
    }

    
    
    
    
    // netty的客户端，连接别的节点
    /**
     * Creates a new instance
     */
    public NetworkManager(ISerializer<T> serializer, MessageListener<T> consumer,
                          int hbInterval, int hbTolerance, int connectTimeout) {
        //Default number of threads for worker groups is (from netty) number of core * 2
        this(serializer, consumer, hbInterval, hbTolerance, connectTimeout, 0);
    }
    
    public NetworkManager(ISerializer<T> serializer, MessageListener<T> consumer,
                          int hbInterval, int hbTolerance, int connectTimeout, int nWorkerThreads) {
        this(serializer, consumer, hbInterval, hbTolerance, connectTimeout, createNewWorkerGroup(nWorkerThreads));
    }
    
    
    //-----------------------------实际的执行者
    
    //TCP通道使用这个创建一个客户端：序列化和反序列化器    consumer:tcp通道       workerGroup是传递过来的一个事件组
    public NetworkManager(ISerializer<T> serializer, MessageListener<T> consumer,
                          int hbInterval, int hbTolerance, int connectTimeout, EventLoopGroup workerGroup) {
        this.serializer = serializer;
        this.consumer = consumer;
        this.hbInterval = hbInterval;
        this.hbTolerance = hbTolerance;
        this.workerGroup = workerGroup;

        this.clientBootstrap = new Bootstrap()
                .channel(channelClass)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
    }
    
    
    
    
    
    
    // 作为netty的客户端，维持了一个向外的连接
    public Connection<T> createConnection(Host peer, Attributes attrs, OutConnListener<T> listener) {
        return new OutConnectionHandler<>(peer, clientBootstrap, listener, consumer,
                serializer, workerGroup.next(), attrs, hbInterval, hbTolerance);
    }

    
    
    
    
    
    
    
    
    // netty的服务端，等待别的节点的连接
    public void createServerSocket(InConnListener<T> l, Host addr, AttributeValidator v, int childThreads) {
        createServerSocket(l, addr, v, childThreads, 1);
    }

    public void createServerSocket(InConnListener<T> l, Host addr, Attributes attr, AttributeValidator v, int childThreads) {
        createServerSocket(l, addr, attr, v, childThreads, 1);
    }

    public void createServerSocket(InConnListener<T> l, Host addr, AttributeValidator v) {
        createServerSocket(l, addr, v, 0, 1);
    }

    public void createServerSocket(InConnListener<T> l, Host addr, Attributes attr, AttributeValidator v) {
        createServerSocket(l, addr, attr, v, 0, 1);
    }

    public void createServerSocket(InConnListener<T> l, Host addr, AttributeValidator v, int childThreads, int parentThreads) {
        createServerSocket(l, addr, Attributes.EMPTY, v, createNewWorkerGroup(childThreads), createNewWorkerGroup(parentThreads));
    }

    public void createServerSocket(InConnListener<T> l, Host addr, Attributes attr, AttributeValidator v, int childThreads, int parentThreads) {
        createServerSocket(l, addr, attr, v, createNewWorkerGroup(childThreads), createNewWorkerGroup(parentThreads));
    }

    public void createServerSocket(InConnListener<T> l, Host addr, AttributeValidator v, EventLoopGroup childGroup) {
        createServerSocket(l, addr, Attributes.EMPTY, v, childGroup, createNewWorkerGroup(1));
    }

    public void createServerSocket(InConnListener<T> l, Host addr, Attributes attr, AttributeValidator v, EventLoopGroup childGroup) {
        createServerSocket(l, addr, attr, v, childGroup, createNewWorkerGroup(1));
    }
    
    
    //创建Netty的实体操作: 实参列表是  TCP通道   (ipv4:port)    空属性组   child-16线程   parent-1线程
    public void createServerSocket(InConnListener<T> listener, Host listenAddr, Attributes attrs, AttributeValidator validator,
                                   EventLoopGroup childGroup, EventLoopGroup parentGroup) {
        //Default number of threads for boss group is 1
        if (serverChannel != null) throw new IllegalStateException("Server socket already created");

        if (attrs == null) throw new IllegalArgumentException("Attributes argument is NULL");

        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup).channel(serverChannelClass);

        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                // 创建消息编码器和解码器
                MessageEncoder<T> encoder = new MessageEncoder<>(serializer);
                MessageDecoder<T> decoder = new MessageDecoder<>(serializer);
                // 如果心跳容忍度或心跳间隔大于0，则添加心跳处理器
                if(hbTolerance > 0 || hbInterval > 0)
                    ch.pipeline().addLast("IdleHandler",
                            new IdleStateHandler(hbTolerance, hbInterval, 0, MILLISECONDS));
                // 添加消息解码器和编码器到管道
                ch.pipeline().addLast("MessageDecoder", decoder);
                ch.pipeline().addLast("MessageEncoder", encoder);
                // 添加握手处理器到管道
                ch.pipeline().addLast("InHandshakeHandler", new InHandshakeHandler(validator, attrs));
                // 添加连接处理器到管道
                ch.pipeline().addLast("InCon",  // 传入的参数是
                        new InConnectionHandler<>(listener, consumer, ch.eventLoop(), attrs, encoder, decoder));
            }
        }); 
        //TODO: study options / child options
        b.option(ChannelOption.SO_BACKLOG, 128);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);
        b.childOption(ChannelOption.TCP_NODELAY, true);

        try {
            serverChannel = b.bind(listenAddr.getAddress(), listenAddr.getPort()).addListener(
                    cf -> listener.serverSocketBind(cf.isSuccess(), cf.cause())).channel();

            serverChannel.closeFuture().addListener(cf -> listener.serverSocketClose(cf.isSuccess(), cf.cause()));
        } catch (Exception e) {
            listener.serverSocketBind(false, e);
        }
    }

    
    
    
    
    
    
    //生成指定线程数的EventLoopGroup
    /**
     * Creates a new {@link EventLoopGroup} using the specified number of threads.
     */
    public static EventLoopGroup createNewWorkerGroup(int nThreads) {
        if (Epoll.isAvailable()) return new EpollEventLoopGroup(nThreads);
        else if (KQueue.isAvailable()) return new KQueueEventLoopGroup(nThreads);
        else return new NioEventLoopGroup(nThreads);
    }

    /**
     * Creates a new {@link EventLoopGroup} using the default number of threads (number of cpus * 2).
     */
    public static EventLoopGroup createNewWorkerGroup() {
        return createNewWorkerGroup(0);
    }
}
