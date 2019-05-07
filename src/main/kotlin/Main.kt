package main

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import protocol.PeerConnection
import tracker.processFile
import java.net.*

// messages for communication with peers
sealed class PeerMsg

data class HasPiece(val id: Int, val has: Boolean, val peer: PeerConnection) : PeerMsg()
data class Closed(val peer: PeerConnection) : PeerMsg()
object Ticker : PeerMsg()
data class DataPiece(val id: Int, val bytes: ByteArray) : PeerMsg()
data class DownloadRequest(val id: Int) : PeerMsg()

data class Piece(val id: Int, var downloaded: Boolean = false, val peers: MutableSet<PeerConnection> = mutableSetOf())


fun main() {
    val (torrent, peers, sha1, peerId, numPieces, pieceLength) = processFile("/Users/dzharvis/Downloads/file.torrent")

    val input = Channel<PeerMsg>(1000) // small buffer just in case

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
        val maxSimultaneousDownloads = 10
        var downloadsInProgess = 0
        // init all pieces
        var piecesToPeers = mutableMapOf<Int, Piece>()
//        val piecesToDownload = mutableSetOf<Piece>()
        for (i in 0 until numPieces) {
            val piece = Piece(i)
//            piecesToDownload.add(piece)
            piecesToPeers[i] = piece
        }

        // as i understood kotlin coroutine can be scheduled on different threads
        // thus we need proper sync event in one coroutine
        // also i don't want an actual actor with a state
        val ticker = ticker(delayMillis = 1000, initialDelayMillis = 0)
        while (true) {
            when (val message = select<PeerMsg>{
                ticker.onReceive {Ticker}
                input.onReceive{it}
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
                    val downloadsStarted = initiateDownloadIfNecessary(
                        piecesToPeers,
                        Math.max(0, maxSimultaneousDownloads - downloadsInProgess)
                    )
                    downloadsInProgess += downloadsStarted
                }
                is Closed -> {
                    for ((_, v) in piecesToPeers) {
                        v.peers.remove(message.peer)

                    }
                }
                is DataPiece -> {
                    downloadsInProgess--
                    println("Piece received! Yay!")
                }
                else -> println(message)
            }
        }
    }

    runBlocking { app.join() }
}

suspend fun initiateDownloadIfNecessary(piecesToPeers: Map<Int, Piece>, amount: Int):Int {
    return piecesToPeers
        .map { (_, piece) -> piece }
        .filter { piece ->
            !piece.downloaded && piece.peers.filter { peer -> !peer.piecesInProgress.contains(piece.id) }.isNotEmpty()
        }
        .shuffled()
        .take(amount)
        .map { piece ->
            val filter = piece.peers.filter { !it.piecesInProgress.contains(piece.id) }
            if (filter.isNotEmpty()){
                val peer = filter.random()
                peer.input.send(DownloadRequest(piece.id))
                1
            } else {
                0
            }
        }.fold(0, {acc, i -> acc + i})
}
