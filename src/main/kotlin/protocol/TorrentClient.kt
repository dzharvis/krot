package protocol

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import main.*
import java.io.InvalidObjectException
import java.lang.Exception
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.ConcurrentSkipListSet

class PeerConnection(
    val addr: InetSocketAddress,
    val output: Channel<PeerMsg>,
    val pieceLength: Int
) {

    @Volatile
    var choked = true
    val piecesInProgress = ConcurrentSkipListSet<Int>()
    val input = Channel<PeerMsg>() // for external events. Should be called only with offer

    private fun log(msg: String) {
        println("[$addr] $msg")
    }

    suspend fun start(sha1: ByteArray, peerId: ByteArray) {
        val channel = AsynchronousSocketChannel.open()
        try {
            val protocol = Protocol(addr, 32384, channel)
            protocol.connect(sha1, peerId)
            coroutineScope {
                // read peer messages
                val peerMessages = produce(capacity = 100) {
                    while (isActive) {
                        when (val element = protocol.readMessage()) {
                            is KeepAlive,
                            is Have,
                            is Bitfield -> {
                                processPayload(element)
                            }
                            else -> {
                                send(element)
                            }
                        }
                    }
                }

                // read supervisor messages and write requests to to peer
                launch {
                    while (isActive) {
                        val message = input.receive()
                        when (message) {
                            is DownloadRequest -> {
                                piecesInProgress.add(message.id)
                                val piece = try {
                                    downloadPiece(message.id, peerMessages, Protocol(addr, 8192, channel))
                                } finally {
                                    piecesInProgress.remove(message.id)
                                }
                                output.send(piece)
                            }
                        }
                    }
                }
            }
        } catch (ex: Exception) {
            log("Disconnected from: $addr, reason: $ex")
            // TODO("Implement retries and reconnects")
            throw ex
        } finally {
            val peer = this
            channel.close()
            withContext(NonCancellable) {
                input.close()
                output.send(Closed(peer))
            }
        }
    }

    private suspend fun waitUnchoke(peerMessages: ReceiveChannel<Message>) {
        val response = peerMessages.receive()
        when (response) {
            is Unchoke -> choked = false
            else -> throw InvalidObjectException("Unchoke expected, $response received")
        }
    }

    private suspend fun downloadPiece(
        id: Int,
        peerMessages: ReceiveChannel<Message>,
        protocol: Protocol
    ): Piece {
        if (choked) {
            // interested byte
            protocol.writeMessage(Interested) // i don't know if i should send interested again after choking
            log("Interested byte sent")

            // wait unchoke
            waitUnchoke(peerMessages)
        }

        // loop request for chunks
        val defaultChunkSize = 16384
        var numChunks = pieceLength / defaultChunkSize // 16KB
        log("Num chunks selected: $numChunks")
        val pieceBytes = ByteBuffer.allocate(pieceLength + defaultChunkSize)
        if (pieceLength % defaultChunkSize != 0) numChunks++
        for (i in 0 until pieceLength step defaultChunkSize) {
            val chunkSize = if (i + defaultChunkSize > pieceLength) (pieceLength - i) else defaultChunkSize
            log("chunk size selected: $chunkSize")
            // wait piece
            log("requesting piece [$pieceLength, $id] $i, $chunkSize")
            protocol.writeMessage(Request(id, i, chunkSize))
            when (val response = peerMessages.receive()) {
                is Chunk -> {
                    log("Chunk received! ${response.begin} ${response.block.size}")
                    pieceBytes.put(response.block)
                }
                is Choke -> {
                    choked = true
                    return downloadPiece(id, peerMessages, protocol) // retry
                }
                else -> throw InvalidObjectException("Chunk expected, $response received")
            }
        }
        pieceBytes.flip()
        val data = ByteArray(pieceBytes.limit())
        log("${data.size} - bytes downloaded")
        pieceBytes.get(data)
        return Piece(id, data)
    }

    private suspend fun processPayload(payload: Message) {
        when (payload) {
            is Bitfield -> { // has parts bitfield
                for (i in 0 until payload.bitField.size) {
                    val currentByte = (payload.bitField[i] + 0) and 0xff
                    for (j in 0 until 8) {
                        val has = ((currentByte ushr (7 - j)) and 1) == 1
                        if (has) output.send(HasPiece(i * 8 + j, has, this))
                    }
                }
            }
            is Have -> { // has parts single piece
                output.send(HasPiece(payload.pieceIndex, true, this))
            }
            is Choke -> choked = true
            is Unchoke -> choked = false
            else -> log("Cannot process message $payload")
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