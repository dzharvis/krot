package protocol;

import async.AsyncTcpClient
import kotlinx.coroutines.withTimeout
import java.lang.IllegalArgumentException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit


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
// extensions
data class DHTRequest(val peerId: ByteArray, val sha1: ByteArray): Message(-1)

enum class Flags {
    DHT
}

// Not thread safe
// Use as thread/coroutine local instance
class Protocol(val addr: InetSocketAddress, bufferSize: Int, val channel: AsynchronousSocketChannel) {

    private val tcpClient = AsyncTcpClient() // uses asyncChannel for most operations, asyncChannels are thread safe
    private val bb = ByteBuffer.allocate(bufferSize)

    private fun log(msg: String) {
        utils.log("[$addr] $msg")
    }

    // returns only after handshake success
    suspend fun connect(sha1: ByteArray, peerId: ByteArray): Set<Flags> {
        // there is no possibility to provide a timeout to an asyncChannel
        // thus set timeout on a coroutine itself
        withTimeout(2000L) { tcpClient.connect(channel, addr) }
        log("Connected to $addr")

        return doHandshake(sha1, peerId)
    }

    suspend fun readMessage(): Message {
        // read length
        bb.clear()
        tcpClient.read(channel, bb, 4)
        bb.flip()
        val length = bb.int

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
        tcpClient.read(channel, bb, bodyLength)
        bb.flip()
        return parse(id.toInt(), bb)
    }

    private fun parse(id: Int, bb: ByteBuffer): Message {
        return when (id) {
            // keep-alive skipped (it doesn't have an id)
            0 -> Choke
            1 -> Unchoke
            2 -> Interested
            3 -> NotInterested
            4 -> {
                val pieceId = bb.int
                Have(pieceId)
            }
            5 -> {
                Bitfield(toByteArray(bb))
            }
            7 -> {
                val index = bb.int
                val begin = bb.int
                // TODO get rid of allocations here
                //  (not sure if it's possible without adding a pool of byte buffers and additional complexity
                //  probably postpone until rewritten to netty
                val block = toByteArray(bb)
                Chunk(index, begin, block)
            }
            else -> {
                log("Unsupported message id $id")
                throw IllegalArgumentException()
            }
        }
    }

    private fun toByteArray(bb: ByteBuffer): ByteArray {
        val body = ByteArray(bb.limit() - bb.position())
        bb.get(body)
        return body
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
    ): Set<Flags> {
        bb.put(19)
        bb.put("BitTorrent protocol".toByteArray(Charset.forName("Windows-1251")))
        bb.put(ByteArray(8))
        bb.put(sha1)
        bb.put(peerId)
        bb.flip()
        tcpClient.write(channel, bb, 5000, TimeUnit.SECONDS)
        bb.clear()

        // read length prefix
        tcpClient.read(channel, bb, 1, 5000, TimeUnit.SECONDS)
        bb.flip()
        val protocolLength = bb.get().toInt() and 0xff
        bb.clear()
        // read handshake response
        tcpClient.read(channel, bb, protocolLength + 8 + 20 + 20, 5000, TimeUnit.SECONDS)
        bb.flip()

        val b = ByteArray(bb.limit())
        bb.get(b)
        log(String(b, Charset.forName("Windows-1251")))
        return parseFlags(b.copyOfRange(protocolLength, protocolLength + 8))
    }

    private fun parseFlags(bytes: ByteArray): Set<Flags> {
        // may add more flags in the future, but currently i'm only interested in dht flag
        if ((bytes[7].toInt() and 1) == 1) {
            return setOf(Flags.DHT)
        }
        return emptySet()
    }
}
