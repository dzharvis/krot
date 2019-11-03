package protocol

import async.AsyncNettyClient
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
import utils.BandwidthCalculator
import java.io.IOException
import java.io.InvalidObjectException
import java.lang.Exception
import java.net.InetSocketAddress

class PeerConnection(
    val addr: InetSocketAddress,
    val output: Channel<SupervisorMsg>
) {
    companion object {
        private const val DEFAULT_BLOCK_SIZE = 16384
    }

    @Volatile
    var choked = true
    val input = Channel<SupervisorMsg>() // for external events. Should be called only with offer
    private val protocolExtensions = mutableSetOf<Flags>()

    private val bandwidth = BandwidthCalculator()
    fun getBandwidth() = bandwidth.getBandwidth()

    private fun log(msg: String) {
        utils.log("[$addr] $msg")
    }

    suspend fun start(sha1: ByteArray, peerId: ByteArray) {
        try {
            //Protocol(addr, 32384, channel)
            val client = AsyncNettyClient.connect(addr)
            protocolExtensions.addAll(client.doHandshake(sha1, peerId))
//            requestPeers(client, sha1, peerId)
            coroutineScope {
                // read peer messages
                val peerMessages = produce(capacity = 100) {
                    while (isActive) {
                        when (val element = client.read()) {
                            is KeepAlive,
                            is Have,
                            is Bitfield -> {
                                log("Processing payload $element")
                                processPayload(element)
                            }
                            else -> {
//                                log("Sending payload $element")
                                send(element)
                            }
                        }
                    }
                }

                // read supervisor messages and write requests to peer
                launch {
                    // 1.5 send keep-alive
                    val ticker = ticker(delayMillis = 1000 * 90, initialDelayMillis = 1000 * 60, mode = TickerMode.FIXED_DELAY)
//                    val protocol = Protocol(addr, 8192, channel)
                    while (isActive) {
                        when (val message = select<SupervisorMsg> {
                            input.onReceive { it }
                            ticker.onReceive { Ticker }
                        }) {
                            is DownloadRequest -> {
                                val piece = try {
                                    downloadPiece(message.id, message.pieceLength, peerMessages, client)
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
                                client.write(KeepAlive)
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
//            client.close()
            withContext(NonCancellable) {
                input.close()
                output.send(Closed(peer))
            }
        }
    }

    private suspend fun requestPeers(protocol: AsyncNettyClient, sha1: ByteArray, peerId: ByteArray) {
        protocol.write(DHTRequest(peerId, sha1))
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
        client: AsyncNettyClient,
        retryCount: Int = 5
    ): Piece {
        if (retryCount == 0) error("Maximum retry count reached")
        if (choked) {
            client.write(Interested) // i don't know if i should send interested again after choking
            log("Interested byte sent")
            waitUnchoke(peerMessages)
            log("unchoke here")
        }

        // loop request for chunks
        val defaultChunkSize = DEFAULT_BLOCK_SIZE.coerceAtMost(pieceLength)
        val pieceBytes = ByteArray(pieceLength)//ByteBuffer.allocate(pieceLength + defaultChunkSize)
        for (i in 0 until pieceLength step defaultChunkSize) {
            val chunkSize = if (i + defaultChunkSize > pieceLength) (pieceLength - i) else defaultChunkSize
            client.write(Request(id, i, chunkSize))
            when (val response = peerMessages.receive()) {
                is Chunk -> {
                    System.arraycopy(response.block, 0, pieceBytes, i, chunkSize)
                    bandwidth.record(response.block.size.toLong())
                }
                is Unchoke -> {
                    log("Unchoke received here. Retry")
                    choked = false
                    return downloadPiece(id, pieceLength, peerMessages, client, retryCount - 1)
                }
                is Choke -> {
                    choked = true
                    return downloadPiece(id, pieceLength, peerMessages, client, retryCount - 1) // stupid retry
                }
                else -> throw InvalidObjectException("Chunk expected, $response received")
            }
        }
        return Piece(id, pieceBytes)
    }

    private suspend fun processPayload(payload: Message) {
        when (payload) {
            is Bitfield -> { // has parts bitfield
                for (i in payload.bitField.indices) {
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
            is KeepAlive -> log("keep-alive received")
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

    override fun toString(): String {
        return "PC[$addr, $choked]"
    }

}
