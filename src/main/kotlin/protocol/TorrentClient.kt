package protocol

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import krot.Closed
import krot.DownloadRequest
import krot.SupervisorMsg
import krot.Ticker
import krot.HasPiece
import krot.Piece
import krot.DownloadCanceledRequest
import java.io.IOException
import java.io.InvalidObjectException
import java.lang.Exception
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel

class PeerConnection(
    val addr: InetSocketAddress,
    val output: Channel<SupervisorMsg>
) {

    @Volatile
    var choked = true
    val input = Channel<SupervisorMsg>() // for external events. Should be called only with offer

    private fun log(msg: String) {
//        println("[$addr] $msg")
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

                // read supervisor messages and write requests to peer
                launch {
                    // 1.5 send keep-alive
                    val ticker = ticker(delayMillis = 1000 * 90, initialDelayMillis = 1000 * 60, mode = TickerMode.FIXED_DELAY)
                    val protocol = Protocol(addr, 8192, channel)
                    while (isActive) {
                        when (val message = select<SupervisorMsg> {
                            input.onReceive { it }
                            ticker.onReceive { Ticker }
                        }) {
                            is DownloadRequest -> {
                                val piece = try {
                                    downloadPiece(message.id, message.pieceLength, peerMessages, protocol)
                                } catch (ex: Exception) {
                                    log("Piece Download was interrupted! ${message.id}")
                                    withContext(NonCancellable) {
                                        output.send(DownloadCanceledRequest(message.id))
                                    }
                                    throw ex
                                }
                                ticker.poll() // remove pending keep-alive just in case
                                output.send(piece)
                            }
                            is Ticker -> {
                                log("Sending keep-alive")
                                protocol.writeMessage(KeepAlive)
                            }
                        }
                    }
                }
            }
        } catch (ex: Exception) {
            log("Disconnected from: $addr, reason: $ex")
            // TODO("Implement retries and reconnects")
            when (ex) {
                is IOException -> {}
                else -> throw ex
            }
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
        when (val response = peerMessages.receive()) {
            is Unchoke -> choked = false
            else -> throw InvalidObjectException("Unchoke expected, $response received")
        }
    }

    private suspend fun downloadPiece(
        id: Int,
        pieceLength: Int,
        peerMessages: ReceiveChannel<Message>,
        protocol: Protocol
    ): Piece {
        if (choked) {
            protocol.writeMessage(Interested) // i don't know if i should send interested again after choking
            log("Interested byte sent")
            waitUnchoke(peerMessages)
            log("unchoke here")
        }

        // loop request for chunks
        val defaultChunkSize = Math.min(pieceLength, 16384)
        val pieceBytes = ByteBuffer.allocate(pieceLength + defaultChunkSize)
        for (i in 0 until pieceLength step defaultChunkSize) {
            val chunkSize = if (i + defaultChunkSize > pieceLength) (pieceLength - i) else defaultChunkSize
            log("chunk size selected: $chunkSize")
            log("requesting piece [$pieceLength, $id] $i, $defaultChunkSize")
            protocol.writeMessage(Request(id, i, chunkSize))
            when (val response = peerMessages.receive()) {
                is Chunk -> {
                    log("Chunk received! ${response.begin} ${response.block.size}")
                    pieceBytes.put(response.block)
                }
                is Choke -> {
                    choked = true
                    // TODO limit number of retries
                    return downloadPiece(id, pieceLength, peerMessages, protocol) // stupid retry
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
                        if (has) output.send(HasPiece(i * 8 + j, this))
                    }
                }
            }
            is Have -> { // has parts single piece
                output.send(HasPiece(payload.pieceIndex, this))
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