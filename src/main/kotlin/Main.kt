package main

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import protocol.PeerConnection
import tracker.processFile
import java.net.*

// messages for communication with peers
sealed class PeerMsg

data class HasPiece(val id: Int, val has: Boolean, val peer: PeerConnection) : PeerMsg()
data class Closed(val peer: PeerConnection) : PeerMsg()
object Ticker : PeerMsg()
data class Piece(val id: Int, val bytes: ByteArray) : PeerMsg()
data class DownloadRequest(val id: Int) : PeerMsg()
data class DownloadCanceledRequest(val id: Int) : PeerMsg()

data class PieceInfo(val id: Int, var downloaded: Boolean = false, val peers: MutableSet<PeerConnection> = mutableSetOf())

fun main() {
    val (torrent, peers, sha1, peerId, numPieces, pieceLength) = processFile("/Users/dzharvis/Downloads/file.torrent")

    println("$numPieces, $pieceLength")

    val input = Channel<PeerMsg>(100) // small buffer just in case

    // start all peers bg process'
    val peerJobs = peers.also { println("Found ${it.size} peers") }.map { (ip, port) ->
        GlobalScope.launch {
            val peer = PeerConnection(InetSocketAddress(ip, port), input, pieceLength)
            peer.start(sha1, peerId)
        }
    }

//    val mutex = Mutex() // suspending mutex
    // supervisor - communicates with peers, download data, etc.
    val app = GlobalScope.launch {
        //TODO use bandwidth check. Slow download speed = increase simultaneous downloads
        val maxSimultaneousDownloads = 20
        var downloadsInProgess = 0
        // init all pieces
        var piecesToPeers = mutableMapOf<Int, PieceInfo>()
        for (i in 0 until numPieces) {
            val piece = PieceInfo(i)
            piecesToPeers[i] = piece
        }

        // as i understood kotlin coroutine can be scheduled on different threads
        // thus we need proper sync event in one coroutine
        // also i don't want an actual actor with a state
        val ticker = ticker(delayMillis = 1000, initialDelayMillis = 0)
        while (true) {
            when (val message = select<PeerMsg> {
                ticker.onReceive { Ticker }
                input.onReceive { it }
            }) {
                is HasPiece -> {
                    val (id, has, peer) = message
                    if (has) {
                        piecesToPeers.get(id)!!.peers.add(peer)
                    } else {
                        piecesToPeers.get(id)!!.peers.remove(peer)
                    }
                    val downloadsStarted = initiateDownloadIfNecessary(
                        piecesToPeers,
                        Math.max(0, maxSimultaneousDownloads - downloadsInProgess)
                    )
                    downloadsInProgess += downloadsStarted
                }
                is Ticker -> {
                    println("Ticker! 1[$downloadsInProgess]")
                    val downloadsStarted = initiateDownloadIfNecessary(
                        piecesToPeers,
                        Math.max(0, maxSimultaneousDownloads - downloadsInProgess)
                    )
                    downloadsInProgess += downloadsStarted
                    println("Ticker! 2[$downloadsInProgess]")
                }
                is Closed -> {
                    for ((_, v) in piecesToPeers) {
                        v.peers.remove(message.peer)
                        downloadsInProgess -= message.peer.piecesInProgress.size

                    }
                }
                is DownloadCanceledRequest -> {
                    downloadsInProgess--
                }
                is Piece -> {
                    downloadsInProgess--
                    println("------ !!! Chunk received! Yay!")
                }
                else -> println(message)
            }
        }
    }

    runBlocking { app.join() }
}

// returns amount of downloads initiated
fun initiateDownloadIfNecessary(piecesToPeers: Map<Int, PieceInfo>, amount: Int): Int {
    return if (amount == 0) 0
    else
        piecesToPeers
            .map { (_, piece) -> Pair(piece, piece.peers.filter { peer -> !peer.piecesInProgress.contains(piece.id) }) }
            .filter { (piece, peers) ->
                !piece.downloaded && peers.isNotEmpty()
            }
            .shuffled()
            .take(amount)
            .map { (piece, peers) ->
                val peer = peers.random()
                if (peer.input.offer(DownloadRequest(piece.id)))
                    1
                else
                    0
            }.fold(0, { acc, i -> acc + i })
}
