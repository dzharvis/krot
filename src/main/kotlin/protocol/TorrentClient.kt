package protocol

import async.AsyncTcpClient
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import main.Closed
import main.HasPiece
import main.PeerMsg
import java.io.IOException
import java.lang.Exception
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

data class Message(val id: Byte, val body: ByteArray)

class PeerConnection(
    val addr: InetSocketAddress,
    val output: Channel<PeerMsg>
) {

    @Volatile
    var choked = true
    val tcpClient = AsyncTcpClient() // uses asyncChannel for most operations, asyncChannel are thread safe
    val input = Channel<PeerMsg>() // for external events

    suspend fun start(sha1: ByteArray, peerId: ByteArray) {
        val channel = AsynchronousSocketChannel.open()
        try {
            // there is not posibility to provide a timeout to an asyncChannel
            // thus set timeout on a coroutine itself
            withTimeout(tcpClient.timeout) { tcpClient.connect(channel, addr) }
            println("Connected to $addr")

            val bb = ByteBuffer.allocate(8192)
            // do a handshake
            bb.put(19)
            bb.put("BitTorrent protocol".toByteArray(Charset.forName("Windows-1251")))
            bb.put(ByteArray(8))
            bb.put(sha1)
            bb.put(peerId)
            bb.flip()
            tcpClient.write(channel, bb)
            bb.clear()

            // read length prefix
            tcpClient.read(channel, bb, 1)
            bb.flip()
            val protocolLength = bb.get().toInt() and 0xff
            bb.clear()
            // read handshake response
            tcpClient.read(channel, bb, protocolLength + 8 + 20 + 20)
            bb.flip()

            val b = ByteArray(bb.limit())
            bb.get(b)
            println(String(b, Charset.forName("Windows-1251")))
            // handshake done!

            tcpClient.timeout = 2 // this seems to be a default timeout for bittorrent protocol
            tcpClient.timeUnit = TimeUnit.MINUTES

            while (true) {
                // todo read input events first
                readInputMessage(bb, channel)
            }
        } catch (ex: Exception) {
            println("Disconnected from: $addr")
            when (ex) {
                is TimeoutCancellationException,
                is IOException -> {
                    // silently disconnect
                    // TODO("Implement retries and reconnects")
                }
                else -> throw ex
            }
        } finally {
            output.send(Closed(this))
            input.close()
            channel.close()
        }
    }

    private suspend fun readInputMessage(bb: ByteBuffer, channel: AsynchronousSocketChannel) {
        bb.clear()
        tcpClient.read(channel, bb, 4)
        bb.flip()
        val length = bb.getInt()

        if (length == 0) { // keep-alive
            // TODO response with keep-alive too
            return
        }

        bb.clear()
        tcpClient.read(channel, bb, 1)
        bb.flip()
        val id = bb.get()

        bb.clear()
        tcpClient.read(channel, bb, length - 1) // length includes message-id byte
        bb.flip()
        val b = ByteArray(bb.limit())
        bb.get(b)
        val payload = Message(id, b)
        processPayload(payload)
    }

    private suspend fun processPayload(payload: Message) {
        when (payload.id) {
            5.toByte() -> { // has parts bitfield
                for (i in 0 until payload.body.size) {
                    val currentByte = (payload.body[i] + 0) and 0xff
                    for (j in 0 until 8) {
                        val has = ((currentByte ushr (7 - j)) and 1) == 1
                        if (has) output.send(HasPiece(i * 8 + j, has, this))
                    }
                }
            }
            4.toByte() -> { // has parts single piece
                val pieceId = ByteBuffer.wrap(payload.body).getInt()
                output.send(HasPiece(pieceId, true, this))
            }
            0.toByte() -> { // choke
                choked = true
            }
            1.toByte() -> { // unchoke
                choked = false
            }
            7.toByte() -> { // piece

            }
            else -> println("Unknown message id ${payload.id}")
        }
    }
}