package protocol;

import async.AsyncTcpClient
import kotlinx.coroutines.withTimeout
import java.lang.IllegalArgumentException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.Charset


data class RawMessage(val id: Byte, val body: ByteArray)

sealed class Message(val id: Byte)
object KeepAlive : Message(-1)
object Choke : Message(0)
object Unchoke : Message(1)
object Interested : Message(2)
object NotInterested : Message(3)
data class Have(val pieceIndex: Int) : Message(4)
data class Bitfield(val bitField: ByteArray) : Message(5)
data class Request(val index: Int, val begin: Int, val length: Int) : Message(6)
data class Chunk(val index: Int, val begin: Int, val block: ByteArray) : Message(7)

// Not thread safe
// Use as thread/coroutine local instance
class Protocol(val addr: InetSocketAddress, bufferSize: Int, val channel: AsynchronousSocketChannel) {

    val tcpClient = AsyncTcpClient() // uses asyncChannel for most operations, asyncChannels are thread safe
    val bb = ByteBuffer.allocate(bufferSize)

    fun log(msg: String) {
//        println("[$addr] $msg")
    }

    // returns only after handshake success
    suspend fun connect(sha1: ByteArray, peerId: ByteArray) {
        // there is no possibility to provide a timeout to an asyncChannel
        // thus set timeout on a coroutine itself
        withTimeout(tcpClient.timeout) { tcpClient.connect(channel, addr) }
        log("Connected to $addr")

        doHandshake(sha1, peerId)
    }

    suspend fun readMessage(): Message {
        // read length
        bb.clear()
        tcpClient.read(channel, bb, 4)
        bb.flip()
        val length = bb.getInt()

        if (length == 0) { // keep-alive
            // TODO respond with keep-alive too
            return KeepAlive
        }

        // read id
        bb.clear()
        tcpClient.read(channel, bb, 1)
        bb.flip()
        val id = bb.get()

        // read body
        bb.clear()
        val bodyLength = length - 1 // length includes message-id byte
        val b = if (bodyLength > 0) {
            tcpClient.read(channel, bb, bodyLength)
            bb.flip()
            val b = ByteArray(bb.limit())
            bb.get(b)
            b
        } else {
            ByteArray(0)
        }
        return parse(RawMessage(id, b))
    }

    private fun parse(m: RawMessage): Message {
        return when (m.id.toInt()) {
            // keep-alive skipped
            0 -> Choke
            1 -> Unchoke
            2 -> Interested
            3 -> NotInterested
            4 -> {
                val pieceId = ByteBuffer.wrap(m.body).int
                Have(pieceId)
            }
            5 -> {
                Bitfield(m.body)
            }
            7 -> {
                val index = ByteBuffer.wrap(m.body, 0, 4).int
                val begin = ByteBuffer.wrap(m.body, 4, 4).int
                val block = m.body.copyOfRange(8, m.body.size)
                Chunk(index, begin, block)
            }
            else -> {
                log("Unsupported message id ${m.id}")
                throw IllegalArgumentException()
            }
        }
    }

    suspend fun writeMessage(message: Message) {
        when (message) {
            is Choke,
            is Unchoke,
            is Interested,
            is NotInterested -> {
                bb.clear()
                bb.putInt(1)
                bb.put(message.id)
                bb.flip()
                tcpClient.write(channel, bb)
            }
            is KeepAlive -> {
                bb.clear()
                bb.putInt(0)
                bb.flip()
                tcpClient.write(channel, bb)
            }
            is Have -> {
                bb.clear()
                bb.putInt(5)
                bb.put(message.id)
                bb.putInt(message.pieceIndex)
                bb.flip()
                tcpClient.write(channel, bb)
            }
            is Request -> {
                bb.clear()
                bb.putInt(13)
                bb.put(message.id)
                bb.putInt(message.index)
                bb.putInt(message.begin)
                bb.putInt(message.length)
                bb.flip()
                tcpClient.write(channel, bb)
            }
            is Bitfield,
            is Chunk -> {
                TODO("I currently do not ever respond with these")
            }
        }
    }

    private suspend fun doHandshake(
        sha1: ByteArray,
        peerId: ByteArray
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
}