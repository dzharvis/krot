package main

import disk.Disk
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import main.progress.Progress
import protocol.PeerConnection
import tracker.TorrentData
import tracker.processFile
import java.net.*
import java.security.MessageDigest
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

// messages for communication with peers
sealed class SupervisorMsg

data class HasPiece(val id: Int, val has: Boolean, val peer: PeerConnection) : SupervisorMsg()
data class Closed(val peer: PeerConnection) : SupervisorMsg()
object Ticker : SupervisorMsg()
data class Piece(val id: Int, val bytes: ByteArray) : SupervisorMsg()
data class DownloadRequest(val id: Int, val pieceLength: Int) : SupervisorMsg()
data class DownloadCanceledRequest(val id: Int) : SupervisorMsg()

data class PieceInfo(
    val id: Int,
    val length: Int,
    var inProgress: Boolean,
    val peers: MutableSet<PeerConnection> = mutableSetOf()
)

object SHA1 {
    fun calculate(bytes: ByteArray): ByteArray {
        val md = MessageDigest.getInstance("SHA-1")
        md.update(bytes, 0, bytes.size)
        return md.digest()!!
    }
}

fun main(args: Array<String>) {

    if (args.size < 2) {
        error("Provide torrent file location as first argument and destination folder as second parameter")
    }
    val file = args[0]
    val dstFolder = args[1]

    val torrentData = processFile(file)
    val (_, peers, sha1, peerId, numPieces, pieceLength, lastPieceLength) = torrentData
    val progress = Progress(numPieces, pieceLength)
    progress.printProgress()
    val input = Channel<SupervisorMsg>(100) // small buffer just in case
    val diskChannel = Channel<Piece>(50)

    progress.state = "Hash check"

    val presentPieces = Disk.checkDownloadedPieces(torrentData, dstFolder, progress).toSet()
    val diskJob = Disk.initWriter(diskChannel, torrentData, dstFolder)

    if (presentPieces.size == numPieces) {
        progress.state = "Done"
        return
    }

    Runtime.getRuntime().addShutdownHook(Thread() {
        println("\nShutting down...")
        try {
            diskChannel.close()
            runBlocking { diskJob.join() }
            Disk.close()
        } catch (ex: Exception) {
        }
    })

    val activePeers = ConcurrentHashMap.newKeySet<PeerConnection>()

    // start all peers bg process'
    val peerJobs = peers.map { (ip, port) ->
        val peer = PeerConnection(InetSocketAddress(ip, port), input)
        GlobalScope.launch {
            peer.start(sha1, peerId)
        }
        activePeers.add(peer)
        peer
    }.toSet()


    // peer requester
    GlobalScope.launch {
        delay(1000 * 60)
        while (isActive) {
            val newPeers = processFile(file).peers.map { (ip, port) ->
                PeerConnection(InetSocketAddress(ip, port), input)
            }.toMutableSet()
            newPeers.removeAll(activePeers)

            newPeers.forEach { p ->
                GlobalScope.launch {
                    p.start(sha1, peerId)
                }
                activePeers.add(p)
            }
            delay(1000 * 60)
        }
    }

    // supervisor - communicates with peers, download data, etc.
    val app = GlobalScope.launch {
        //TODO use bandwidth check. Slow download speed = increase simultaneous downloads
        val maxSimultaneousDownloads = 40
        var downloadsInProgress = 0
        // init all pieces
        val piecesToPeers = mutableMapOf<Int, PieceInfo>()
        for (i in 0 until numPieces) {
            if (!presentPieces.contains(i)) {
                val piece =
                    PieceInfo(i, if (i == numPieces - 1) lastPieceLength.toInt() else pieceLength.toInt(), false)
                piecesToPeers[i] = piece
            }
        }

        progress.state = "Downloading"
        val ticker = ticker(delayMillis = 1000, initialDelayMillis = 0, mode = TickerMode.FIXED_DELAY)
        while (true) {
            when (val message = select<SupervisorMsg> {
                ticker.onReceive { Ticker }
                input.onReceive { it }
            }) {
                is HasPiece -> {
                    val (id, has, peer) = message
                    if (has) {
                        piecesToPeers[id]?.peers?.add(peer)
                    } else {
                        piecesToPeers[id]?.peers?.remove(peer)
                    }
                    val downloads = initiateDownloadIfNecessary(
                        piecesToPeers,
                        maxSimultaneousDownloads,
                        downloadsInProgress
                    )
                    downloadsInProgress += downloads.size
                    for (d in downloads) {
                        progress.setInProgress(d)
                    }

                }
                is Ticker -> {
                    val downloads = initiateDownloadIfNecessary(
                        piecesToPeers,
                        maxSimultaneousDownloads,
                        downloadsInProgress
                    )
                    downloadsInProgress += downloads.size
                    for (d in downloads) {
                        progress.setInProgress(d)
                    }

                }
                is Closed -> {
                    for ((_, v) in piecesToPeers) {
                        v.peers.remove(message.peer)
                    }
                    activePeers.remove(message.peer)
                    progress.peers = activePeers.size
                }
                is DownloadCanceledRequest -> {
                    downloadsInProgress--
                    piecesToPeers[message.id]?.inProgress = false
                }
                is Piece -> {
                    progress.peers = activePeers.size
                    downloadsInProgress--
                    val hashEqual = computePieceHash(message, torrentData)
                    if (!hashEqual) {
                        piecesToPeers[message.id]!!.inProgress = false
                        progress.setEmpty(message.id)
                    } else {
                        piecesToPeers.remove(message.id)
                        diskChannel.send(message)
                        progress.setDone(message.id)
                        if (piecesToPeers.isEmpty()) {
                            progress.state = "Done"
                            diskChannel.close()
                            return@launch
                        }
                    }
                }
                else -> println(message)
            }
        }
    }

    runBlocking {
        diskJob.join()
        Disk.close()
        app.join()
    }
}

fun computePieceHash(message: Piece, torrentData: TorrentData): Boolean {
    val expectedHash =
        torrentData.torrent["info"]!!.map["pieces"]!!.bytes.copyOfRange(message.id * 20, message.id * 20 + 20)
    val pieceHash = SHA1.calculate(message.bytes)
    return Arrays.equals(expectedHash, pieceHash)
}

// returns amount of downloads initiated
fun initiateDownloadIfNecessary(
    piecesToPeers: Map<Int, PieceInfo>,
    maxSimultaneousDownloads: Int,
    downloadsInProgress: Int
): List<Int> {
    val amount = maxSimultaneousDownloads - downloadsInProgress
    return if (amount == 0) emptyList()
    else
        piecesToPeers
            .map { (_, piece) -> piece }
            .filter { piece ->
                !piece.inProgress && piece.peers.isNotEmpty()
            }
            .shuffled() // very expensive operation
            .take(amount)
            .map { piece ->
                val peer = piece.peers.random()
                val offer = try {
                    peer.input.offer(DownloadRequest(piece.id, piece.length))
                } catch (ex: ClosedSendChannelException) {
                    false
                }
                if (offer) {
                    piece.inProgress = true
                    piece.id
                } else
                    -1
            }.filterNot { it == -1 }
}
