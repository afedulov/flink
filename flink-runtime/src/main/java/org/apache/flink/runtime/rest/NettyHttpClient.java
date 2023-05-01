package org.apache.flink.runtime.rest;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufUtil;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpUtil;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LoggingHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslContextBuilder;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.URI;

public class NettyHttpClient {
    public static void main(String[] args) throws Exception {
        //        URI uri = new URI("https://example.com/");
        //        URI uri = new URI("https://surface-test.aws.ocean.g.apple.com");
        //        URI uri = new
        // URI("https://surface-test.aws.ocean.g.apple.com/swagger-ui/index.html");
        URI uri = new URI("https://og60-surface-eks-12.aws.ocean.g.apple.com/v1/info");
        String host = uri.getHost();
        int port = uri.getPort() == -1 ? 443 : uri.getPort();

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // Configure SSL context
            SslContext sslContext =
                    SslContextBuilder.forClient()
                            // .trustManager(InsecureTrustManagerFactory.INSTANCE)
                            .build();

            // Configure the client
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(
                            new ChannelInitializer<Channel>() {
                                @Override
                                protected void initChannel(Channel ch) {
                                    System.out.println("Initializing channel");
                                    ch.pipeline()
                                            .addLast(
                                                    new LoggingHandler(LogLevel.INFO),
                                                    //
                                                    //      sslContext.newHandler(ch.alloc(), host,
                                                    // port),
                                                    new DynamicSslHandler(sslContext),
                                                    new HttpClientCodec(),
                                                    new HttpContentDecompressor(),
                                                    new InboundTrafficLogger(),
                                                    new HttpResponseHandler());
                                }
                            });

            // Start the client
            Channel channel = bootstrap.connect(host, port).sync().channel();

            // Prepare the HTTP request
            DefaultFullHttpRequest request =
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.GET,
                            uri.getRawPath(),
                            Unpooled.EMPTY_BUFFER);

            request.headers().set(HttpHeaderNames.HOST, host);
            request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            request.headers().set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);

            // Send the HTTP request
            channel.writeAndFlush(request);

            // Wait for the server to close the connection
            channel.closeFuture().sync();
        } finally {
            // Shut down the event loop group
            group.shutdownGracefully();
        }
    }

    public static class HttpResponseHandler extends SimpleChannelInboundHandler<HttpObject> {

        private boolean readingChunks = false;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                System.out.println("Status: " + response.status());
                System.out.println("Headers: " + response.headers());

                if (HttpUtil.isTransferEncodingChunked(response)) {
                    readingChunks = true;
                    System.out.println("Receiving data in chunks.");
                } else {
                    readingChunks = false;
                }
            } else if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;

                System.out.println("Chunk: " + content.content().toString(CharsetUtil.UTF_8));

                if (content instanceof LastHttpContent) {
                    System.out.println("End of content");
                    readingChunks = false;
                    ctx.close();
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public static class InboundTrafficLogger extends ChannelDuplexHandler {
        //        @Override
        //        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //            if (msg instanceof ByteBuf) {
        //                System.out.println("Received bytes: " +
        // ByteBufUtil.prettyHexDump((ByteBuf) msg));
        //            } else {
        //                System.out.println("Received object: " + msg);
        //            }
        //            super.channelRead(ctx, msg);
        //        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                System.out.println(">>> Received FullHttpResponse:");
                System.out.println(response.toString());
                ByteBuf content = response.content();
                if (content.isReadable()) {
                    System.out.println("Content: " + content.toString(CharsetUtil.UTF_8));
                }
            } else if (msg instanceof ByteBuf) {
                System.out.println(
                        ">>> Received bytes: " + ByteBufUtil.prettyHexDump((ByteBuf) msg));
            } else {
                System.out.println(">>> Received object: " + msg);
            }
            super.channelRead(ctx, msg);
        }
    }

    public static class DynamicSslHandler extends ChannelInboundHandlerAdapter {
        private final SslContext sslContext;

        public DynamicSslHandler(SslContext sslContext) {
            this.sslContext = sslContext;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
            String host = address.getHostString();
            int port = address.getPort();

            SslHandler sslHandler = sslContext.newHandler(ctx.alloc(), host, port);
            ctx.pipeline().addBefore(ctx.name(), null, sslHandler);

            ctx.fireChannelActive();
        }
    }
}
