package protocol

import async.AsyncTcpClient
import kotlinx.coroutines.*
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withTimeout
import main.*
import java.io.IOException
import java.io.InvalidObjectException
import java.lang.Exception
import java.lang.IllegalArgumentException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.TimeUnit

data class RawMessage(val id: Byte, val body: ByteArray)

sealed class Message()
object KeepAlive : Message()
object Choke : Message() {
    val id: Int = 0
}

object Unchoke : Message() {
    val id: Int = 1
}

object Interested : Message() {
    val id: Int = 2
}

object NotInterested : Message() {
    val id: Int = 3
}

data class Have(val message: RawMessage) : Message() {
    val id: Int = 4
}

data class Bitfield(val message: RawMessage) : Message() {
    val id: Int = 5
}

data class Request(val message: RawMessage) : Message() {
    val id: Int = 6
}

data class Piece(val message: RawMessage) : Message() {
    val id: Int = 7
}
class PeerConnection(
    val addr: InetSocketAddress,
    val output: Channel<PeerMsg>,
    val pieceLength: Int
) {

    @Volatile
    var choked = true
    val tcpClient = AsyncTcpClient() // uses asyncChannel for most operations, asyncChannel are thread safe
    val input = Channel<PeerMsg>(50) // for external events
    val piecesInProgress = ConcurrentSkipListSet<Int>()

    fun log(msg: String) {
        println("[$addr] $msg")
    }

    suspend fun start(sha1: ByteArray, peerId: ByteArray) {
        val channel = AsynchronousSocketChannel.open()
        try {
            // there is not posibility to provide a timeout to an asyncChannel
            // thus set timeout on a coroutine itself
            withTimeout(tcpClient.timeout) { tcpClient.connect(channel, addr) }
            log("Connected to $addr")

            val bb = ByteBuffer.allocate(8192)
            doHandshake(bb, sha1, peerId, channel)

            tcpClient.timeout = 2 // this seems to be a default timeout for bittorrent protocol
            tcpClient.timeUnit = TimeUnit.MINUTES

            coroutineScope {
                // read peer messages
                val bb = ByteBuffer.allocate(32384)
                val peerMessages = produce(capacity = 100) {
                    while (isActive) {
                        log("Read started")
                        val element = readInputMessage(bb, channel)
                        log("Recevied: $element.")
                        when (element) {
                            is KeepAlive,
                            is Have,
                            is Bitfield -> {
                                log("-Processing started: $element")
                                processPayload(element)
                                log("-Processing ended: $element")
                            }
                            else -> {
                                log("sending: $element")
                                send(element)
                            }
                        }
                    }
                }

                // read supervisor messages
                launch {
                    val bb = ByteBuffer.allocate(8192)
                    while (isActive) {
                        val message = input.receive()
                        when (message) {
                            is DownloadRequest -> {
                                val piece = downloadPiece(message.id, peerMessages, bb, channel)
                                output.send(piece)
                            }
                        }
                    }
                }
            }
        } catch (ex: Exception) {
            log("Disconnected from: $addr, reason: $ex")
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

    private suspend fun doHandshake(
        bb: ByteBuffer,
        sha1: ByteArray,
        peerId: ByteArray,
        channel: AsynchronousSocketChannel
    ) {
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
        log(String(b, Charset.forName("Windows-1251")))
    }

    suspend private fun downloadPiece(
        id: Int,
        peerMessages: ReceiveChannel<Message>,
        bb: ByteBuffer,
        channel: AsynchronousSocketChannel
    ): DataPiece {
        piecesInProgress.add(id)
        try {
            // interested byte
            bb.clear()
            bb.putInt(1)
            bb.put(2)
            bb.flip()
            tcpClient.write(channel, bb)
            log("Interested byte sent")

            // wait unchoke
            val response = peerMessages.receive()
            log("resonse received $response")
            when (response) {
                is Unchoke -> choked = false
                else -> throw InvalidObjectException("Unchoke expected, $response received")
            }
            // loop request for pieces

            val defaultChunkSize = 16384
            var numChunks = pieceLength / defaultChunkSize // 16KB
            log("Num chunks selected: $numChunks")
            if (pieceLength % defaultChunkSize != 0) numChunks++
            for (i in 0 until pieceLength step defaultChunkSize) {
                bb.clear()
                bb.putInt(13)
                bb.put(6)
                bb.putInt(id)
                bb.putInt(i)
                val chunkSize = if (i + defaultChunkSize > pieceLength) (pieceLength - i) else defaultChunkSize
                log("chunk size selected: $chunkSize")
                bb.putInt(chunkSize)
                bb.flip()
                // wait piece
                log("requesting piece")
                tcpClient.write(channel, bb)
                val response = peerMessages.receive()
                log("received piece !")
                when (response) {
                    is Piece -> log("Piece received!")
                    else -> throw InvalidObjectException("Piece expected, $response received")
                }
            }
        } finally {
            piecesInProgress.remove(id)
        }
        TODO()
    }

    private suspend fun readInputMessage(bb: ByteBuffer, channel: AsynchronousSocketChannel): Message {
        bb.clear()
        tcpClient.read(channel, bb, 4)
        bb.flip()
        val length = bb.getInt()

        if (length == 0) { // keep-alive
            // TODO response with keep-alive too
            return KeepAlive
        }

        log("not keep-alive read started")

        bb.clear()
        tcpClient.read(channel, bb, 1)
        bb.flip()
        val id = bb.get()

        bb.clear()
        tcpClient.read(channel, bb, length - 1) // length includes message-id byte
        bb.flip()
        val b = ByteArray(bb.limit())
        bb.get(b)
        val m = RawMessage(id, b)
        return parse(m)
//        processPayload(payload)
    }

    private fun parse(m: RawMessage): Message {
        return when (m.id.toInt()) {
            // keep-alive skipped
            0 -> Choke
            1 -> Unchoke
            4 -> Have(m)
            5 -> Bitfield(m)
            7 -> Piece(m)
            else -> {
                log("Unknown message id ${m.id}")
                throw IllegalArgumentException()
            }
        }
    }

    private suspend fun processPayload(payload: Message) {
        when (payload) {
            is Bitfield -> { // has parts bitfield
                for (i in 0 until payload.message.body.size) {
                    val currentByte = (payload.message.body[i] + 0) and 0xff
                    for (j in 0 until 8) {
                        val has = ((currentByte ushr (7 - j)) and 1) == 1
                        if (has) output.send(HasPiece(i * 8 + j, has, this))
                    }
                }
            }
            is Have -> { // has parts single piece
                val pieceId = ByteBuffer.wrap(payload.message.body).int
                output.send(HasPiece(pieceId, true, this))
            }
            is Choke -> choked = true
            is Unchoke -> choked = false
            is Piece -> { // piece

            }
            else -> log("Unknown message $payload")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PeerConnection

        if (addr != other.addr) return false

        return true
    }

    override fun hashCode(): Int {
        return addr.hashCode()
    }

}