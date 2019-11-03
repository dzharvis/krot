package async

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.sendBlocking
import protocol.*
import utils.log
import java.lang.IllegalArgumentException
import java.lang.RuntimeException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class Encoder : ChannelOutboundHandlerAdapter() {
    override fun write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
        val message = msg as Message
//        println("Encoding $message")
        when (message) {
            is Choke,
            is Unchoke,
            is Interested,
            is NotInterested -> {
                val bb = ctx.alloc().buffer(5)
                bb.writeInt(1)
                bb.writeByte(message.id.toInt())
                ctx.writeAndFlush(bb)
//                bb.release()
            }
            is KeepAlive -> {
                val bb = ctx.alloc().buffer(4)
                bb.writeInt(0)
                ctx.writeAndFlush(bb)
//                bb.release()
            }
            is Have -> {
                val bb = ctx.alloc().buffer(9)
                bb.writeInt(5)
                bb.writeByte(message.id.toInt())
                bb.writeInt(message.pieceIndex)
                ctx.writeAndFlush(bb)
//                bb.release()
            }
            is Request -> {
                val bb = ctx.alloc().buffer(17)
                bb.writeInt(13)
                bb.writeByte(message.id.toInt())
                bb.writeInt(message.index)
                bb.writeInt(message.begin)
                bb.writeInt(message.length)
                ctx.writeAndFlush(bb)
//                bb.release()
            }
            is Raw -> {
                val bb = PooledByteBufAllocator.DEFAULT.buffer()
                bb.writeBytes(message.bytes)
                ctx.writeAndFlush(bb)
//                bb.release()
            }
            is Bitfield,
            is Chunk -> {
                TODO("I currently do not ever respond with these")
            }
        }
    }
}

class Decoder(private val output: kotlinx.coroutines.channels.Channel<Message>) : ChannelInboundHandlerAdapter() {
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val m = (msg as ByteBuf)
        val length = m.readInt()
        if (length == 0) {
//            m.release()
//            println("len zero")
            require(output.offer(KeepAlive))
            return
        }
//        if(m.readableBytes() < length) {
//            m.resetReaderIndex()
//            println("waiting for bytes")
//            return
//        }

        val id = m.readByte().toInt()

        val resp = when (id) {
            // keep-alive skipped (it doesn't have an id)
            0 -> Choke
            1 -> Unchoke
            2 -> Interested
            3 -> NotInterested
            4 -> {
                val pieceId = m.readInt()
                Have(pieceId)
            }
            5 -> {
                Bitfield(m.toByteArray(length - 1))
            }
            7 -> {
                val index = m.readInt()
                val begin = m.readInt()
                // TODO get rid of allocations here
                //  (not sure if it's possible without adding a pool of byte buffers and additional complexity
                //  probably postpone until rewritten to netty
                val block = m.toByteArray(length - 1 - 8)
                Chunk(index, begin, block)
            }
            else -> {
                log("Unsupported message id $id")
                throw IllegalArgumentException()
            }
        }
        m.release()
//        println(resp)

        GlobalScope.launch { require(output.offer(resp)) }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
        output.close(cause)
        cause.printStackTrace()
    }
}

class HandshakeHandler(private val output: kotlinx.coroutines.channels.Channel<Message>) :
    ChannelInboundHandlerAdapter() {
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
//        println("Handshake")
        val m = (msg as ByteBuf)
        // read length prefix
        val protocolLength = m.readUnsignedByte().toInt()
        if (m.readableBytes() < protocolLength + 8 + 20 + 20) {
            m.resetReaderIndex()
//            m.release()
            return
        }
//        println(protocolLength)
//        println(m.readableBytes())
        // read handshake response
        val b = m.toByteArray(protocolLength + 8 + 20 + 20)

        log(String(b, Charset.forName("Windows-1251")))
//        return parseFlags(b.copyOfRange(protocolLength, protocolLength + 8))
//        m.release()
//        ctx.fireChannelReadComplete()
        ctx.pipeline().remove(this)
        require(output.offer(Raw(b)))
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        ctx.close()
        output.close(cause)
        cause.printStackTrace()
    }
}

class AsyncNettyClient(
    private val channel: Channel,
    private val output: kotlinx.coroutines.channels.Channel<Message>
) {

    suspend fun read() = withTimeoutOrNull(5000) { output.receive() } ?: run {
        channel.close()
        output.close(RuntimeException("Channel closed"))
        throw RuntimeException("Channel closed")
    }
    suspend fun write(msg: Message) {
        require(channel.isActive && channel.isWritable)
        val write = channel.write(msg)
//        return suspendCancellableCoroutine { cont ->
//            write.addListener {
//                if (it.isDone && it.isSuccess)
//                    cont.resume(Unit)
//                else
//                    cont.resumeWithException(it.cause())
//            }
//        }
    }

    suspend fun doHandshake(
        sha1: ByteArray,
        peerId: ByteArray
    ): Set<Flags> {
        val bb = PooledByteBufAllocator.DEFAULT.buffer()
        bb.writeByte(19)
        bb.writeBytes("BitTorrent protocol".toByteArray(Charset.forName("Windows-1251")))
        bb.writeBytes(ByteArray(8))
        bb.writeBytes(sha1)
        bb.writeBytes(peerId)
        channel.writeAndFlush(Raw(bb.toByteArray(bb.readableBytes())))
        bb.release()
        val resp = read() as Raw
        return parseFlags(resp.bytes.copyOfRange(resp.bytes.size - 48, resp.bytes.size - 40))
    }

    suspend fun close() {
        output.close()
        channel.close()
        return suspendCancellableCoroutine { cont ->
            channel.closeFuture().addListener {
                cont.resume(Unit)
            }
        }
    }

    companion object {
        private val workerGroup = NioEventLoopGroup()
        suspend fun connect(addr: InetSocketAddress): AsyncNettyClient {
            val output = kotlinx.coroutines.channels.Channel<Message>(10)
            val b = Bootstrap()
            b.group(workerGroup)
            b.channel(NioSocketChannel::class.java)
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            b.option(ChannelOption.SO_KEEPALIVE, true)
            b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            b.handler(object : ChannelInitializer<SocketChannel>() {
                public override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(HandshakeHandler(output))
                    ch.pipeline().addLast(LengthFieldBasedFrameDecoder(256 * 1025, 0, 4))
                    ch.pipeline().addLast(Encoder())
                    ch.pipeline().addLast(Decoder(output))
                }
            })
            val connect = b.connect(addr.hostName, addr.port)
            return suspendCancellableCoroutine { cont ->
                connect.addListener {
                    if (it.isDone && it.isSuccess)
                        cont.resume(AsyncNettyClient((it as ChannelFuture).channel(), output))
                    else
                        cont.resumeWithException(it.cause())
                }
            }

//        // Start the client.
//        chan = .sync() // (5)
//
//        f.channel().
//
//            // Wait until the connection is closed.
//            f.channel().closeFuture().sync()
        }


    }

    private fun parseFlags(bytes: ByteArray): Set<Flags> {
        // may add more flags in the future, but currently i'm only interested in dht flag
        if ((bytes[7].toInt() and 1) == 1) {
            return setOf(Flags.DHT)
        }
        return emptySet()
    }
}

private fun ByteBuf.toByteArray(len: Int): ByteArray {
    val body = ByteArray(len)
    this.readBytes(body)
    return body
}