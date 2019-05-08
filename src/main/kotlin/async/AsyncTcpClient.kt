package async

import kotlinx.coroutines.suspendCancellableCoroutine
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.CompletionHandler
import java.util.concurrent.TimeUnit
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

// Not thread safe
// Use as thread/coroutine local instance
class AsyncTcpClient {

    @Volatile
    var timeout = 2000L
    @Volatile
    var timeUnit = TimeUnit.MILLISECONDS

    class AsyncCompletionHandler<T> : CompletionHandler<T, Continuation<T>> {
        override fun completed(result: T, attachment: Continuation<T>) = attachment.resume(result)
        override fun failed(exc: Throwable, attachment: Continuation<T>) {
            println("Exception happened $exc")
            attachment.resumeWithException(exc)
        }
    }

    suspend fun connect(ch: AsynchronousSocketChannel, addr: InetSocketAddress): Void {
        return suspendCancellableCoroutine { continuation ->
            ch.connect(addr, continuation, AsyncCompletionHandler<Void>())
        }
    }

    suspend fun read(ch: AsynchronousSocketChannel, bb: ByteBuffer): Int {
        return suspendCancellableCoroutine { continuation ->
            ch.read(bb, timeout, timeUnit, continuation, AsyncCompletionHandler<Int>())
        }
    }

    suspend fun read(ch: AsynchronousSocketChannel, bb: ByteBuffer, n: Int) {
        val slice = bb.slice()
        slice.limit(n)
        while (slice.hasRemaining()) {
            if (read(ch, slice) < 0) {
                throw ClosedChannelException()
            }
        }
        bb.position(bb.position() + n)
    }

    suspend fun readFull(ch: AsynchronousSocketChannel, bb: ByteBuffer) {
        while (bb.hasRemaining()) {
            if (read(ch, bb) < 0) {
                throw ClosedChannelException()
            }
        }
    }

    suspend fun write(ch: AsynchronousSocketChannel, bb: ByteBuffer): Int {
        return suspendCancellableCoroutine { continuation ->
            ch.write(bb, timeout, timeUnit, continuation, AsyncCompletionHandler<Int>())
        }
    }
}