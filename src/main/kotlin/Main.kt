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
import tracker.processFile
import java.net.*

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

fun main() {

    val torrentData = processFile("/Users/dzharvis/Downloads/winrar.torrent")
    val (torrent, peers, sha1, peerId, numPieces, pieceLength, lastPieceLength) = torrentData

    val input = Channel<SupervisorMsg>(100) // small buffer just in case

    val diskChannel = Channel<Piece>(50)

    val diskJob = Disk.initWriter(diskChannel, torrentData)

    println("Found ${peers.size} peers")
    // start all peers bg process'
    val peerJobs = peers.map { (ip, port) ->
        GlobalScope.launch {
            val peer = PeerConnection(InetSocketAddress(ip, port), input)
            peer.start(sha1, peerId)
        }
    }

    // supervisor - communicates with peers, download data, etc.
    val app = GlobalScope.launch {
        //TODO use bandwidth check. Slow download speed = increase simultaneous downloads
        val maxSimultaneousDownloads = 20
        var downloadsInProgess = 0
        // init all pieces
        var piecesToPeers = mutableMapOf<Int, PieceInfo>()
        for (i in 0 until numPieces) {
            val piece = PieceInfo(i, if (i == numPieces - 1) lastPieceLength else pieceLength, false)
            piecesToPeers[i] = piece
        }

        val progress = Progress(numPieces, pieceLength)
        val ticker = ticker(delayMillis = 1000, initialDelayMillis = 0, mode = TickerMode.FIXED_DELAY)
        while (true) {
            when (val message = select<SupervisorMsg> {
                ticker.onReceive { Ticker }
                input.onReceive { it }
            }) {
                is HasPiece -> {
                    val (id, has, peer) = message
                    if (has) {
                        piecesToPeers.get(id)?.peers?.add(peer)
                    } else {
                        piecesToPeers.get(id)?.peers?.remove(peer)
                    }
                    val downloads = initiateDownloadIfNecessary(
                        piecesToPeers,
                        maxSimultaneousDownloads,
                        downloadsInProgess
                    )
                    downloadsInProgess += downloads.size
                    for (d in downloads) {
                        progress.setInProgress(d)
                    }

                }
                is Ticker -> {
                    val downloads = initiateDownloadIfNecessary(
                        piecesToPeers,
                        maxSimultaneousDownloads,
                        downloadsInProgess
                    )
                    downloadsInProgess += downloads.size
                    for (d in downloads) {
                        progress.setInProgress(d)
                    }
                }
                is Closed -> {
                    for ((_, v) in piecesToPeers) {
                        v.peers.remove(message.peer)
                    }
                }
                is DownloadCanceledRequest -> {
                    downloadsInProgess--
                    piecesToPeers[message.id]?.inProgress = false
                }
                is Piece -> {
                    downloadsInProgess--
                    piecesToPeers.remove(message.id)
                    diskChannel.send(message)
                    progress.setDone(message.id)
                    printProgress(progress)
                    if (piecesToPeers.isEmpty()) {
                        diskChannel.close()
                        return@launch
                    }
                }
                else -> println(message)
            }
        }
    }

    runBlocking {
        diskJob.join()
        app.join()
    }
}

private fun printProgress(progress: Progress) {
    System.out.print("\r")
    System.out.print("${progress.getProgressPercent()} ${progress.getProgressString()} ${progress.getBandwidth()}")
    System.out.flush()
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
            .map { (id, piece) -> piece }
            .filter { piece ->
                !piece.inProgress && piece.peers.isNotEmpty()
            }
            .shuffled()
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
