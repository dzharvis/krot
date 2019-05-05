package main

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import protocol.PeerConnection
import tracker.processFile
import java.net.*

// messages for communication with peers
sealed class PeerMsg

data class HasPiece(val id: Int, val has: Boolean, val peer: PeerConnection) : PeerMsg()
data class Closed(val peer: PeerConnection) : PeerMsg()

data class Piece(val id: Int, var downloaded: Boolean = false, val peers: MutableSet<PeerConnection> = mutableSetOf())

fun main() {
    val (torrent, peers, sha1, peerId, numPieces) = processFile("/Users/dzharvis/Downloads/file.torrent")

    var piecesToPeers = mutableMapOf<Int, Piece>()
    val input = Channel<PeerMsg>(100) // small buffer just in case

    // start all peers bg process'
    val peerJobs = peers.take(15).map { (ip, port) ->
        GlobalScope.launch {
            val peer = PeerConnection(InetSocketAddress(ip, port), input)
            peer.start(sha1, peerId)
        }
    }

    // init all pieces
    val piecesToDownload = mutableListOf<Piece>()
    for (i in 0 until numPieces) {
        val piece = Piece(i)
        piecesToDownload.add(piece)
        piecesToPeers[i] = piece
    }

    // supervisor - communicates with peers, download data, etc.
    val app = GlobalScope.launch {
        // as i understood kotlin coroutine can be scheduled on different threads
        // thus we need proper sync event in one coroutine
        // also i don't want an actual actor with a state
        val mutex = Mutex() // suspending mutex
        for (message in input) {
            when (message) {
                is HasPiece -> {
                    mutex.withLock {
                        val (id, has, peer) = message
                        if (has) {
                            piecesToPeers.get(id)!!.peers.add(peer)
                        } else {
                            piecesToPeers.get(id)!!.peers.remove(peer)
                        }
                    }
                }
                is Closed -> {
                    for ((_, v) in piecesToPeers) {
                        mutex.withLock {
                            v.peers.remove(message.peer)
                        }
                    }
                }
                else -> println(message)
            }

            println(piecesToPeers)
        }
    }

    runBlocking { app.join() }
}
