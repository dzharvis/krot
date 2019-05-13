package krot

import disk.Disk
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import main.progress.Progress
import protocol.PeerConnection
import tracker.Tracker
import utils.getPieceSha1
import utils.sha1
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// messages for communication with peers
sealed class SupervisorMsg

data class HasPiece(val id: Int, val peer: PeerConnection) : SupervisorMsg()
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

class Krot(val disk: Disk, val tracker: Tracker) {

    private val input = Channel<SupervisorMsg>(100) // small buffer just in case
    private val maxSimultaneousDownloads = 40

    fun start() {
        val (pieceLength, _, numPieces) = tracker.torrentData
        val progress = Progress(numPieces, pieceLength)
        progress.printProgress()

        progress.state = "Hash check"
        val presentPieces = disk.checkDownloadedPieces(progress)
        if (presentPieces.size == numPieces) {
            progress.state = "Done"
            return
        }

        val activePeers = ConcurrentHashMap.newKeySet<PeerConnection>()
        startPeerFinder(activePeers)

        // supervisor - communicates with peers, download data, etc.
        val app = startSupervisor(presentPieces, progress, activePeers)

        runBlocking {
            app.join()
            disk.shutdown()
        }
    }

    private fun startSupervisor(
        presentPieces: Set<Int>,
        progress: Progress,
        activePeers: MutableSet<PeerConnection>
    ): Job {
        val (pieceLength, lastPieceLength, numPieces) = tracker.torrentData
        return GlobalScope.launch {
            //TODO use bandwidth check. Slow download speed = increase simultaneous downloads
            var downloadsInProgress = 0
            // init all pieces
            val piecesToPeers = mutableMapOf<Int, PieceInfo>()
            val absentPieces = (0 until numPieces).filterNot(presentPieces::contains)
            for (i in absentPieces) {
                val pieceLength = if (i == numPieces - 1) lastPieceLength.toInt() else pieceLength.toInt()
                piecesToPeers[i] = PieceInfo(i, pieceLength, false)
            }

            disk.initWriter()
            progress.state = "Downloading"
            val ticker = ticker(delayMillis = 1000, initialDelayMillis = 0, mode = TickerMode.FIXED_DELAY)
            while (true) {
                when (val message = select<SupervisorMsg> {
                    ticker.onReceive { Ticker }
                    input.onReceive { it }
                }) {
                    is HasPiece -> {
                        piecesToPeers[message.id]?.peers?.add(message.peer)
                        val downloads = initiateDownloadIfNecessary(piecesToPeers, downloadsInProgress)
                        downloadsInProgress += downloads.size
                        for (d in downloads) {
                            progress.setInProgress(d)
                        }

                    }
                    is Ticker -> {
                        val downloads = initiateDownloadIfNecessary(piecesToPeers, downloadsInProgress)
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
                        progress.numPeers = activePeers.size
                    }
                    is DownloadCanceledRequest -> {
                        downloadsInProgress--
                        piecesToPeers[message.id]?.inProgress = false
                    }
                    is Piece -> {
                        progress.numPeers = activePeers.size
                        downloadsInProgress--
                        val hashEqual = isPieceValid(message)
                        if (!hashEqual) {
                            piecesToPeers[message.id]?.inProgress = false
                            progress.setEmpty(message.id)
                        } else {
                            piecesToPeers.remove(message.id)
                            disk.input.send(message)
                            progress.setDone(message.id)
                            if (piecesToPeers.isEmpty()) {
                                progress.state = "Done"
                                disk.input.close()
                                return@launch
                            }
                        }
                    }
                    else -> error(message)
                }
            }
        }
    }

    private fun startPeerFinder(
        activePeers: MutableSet<PeerConnection>
    ) {
        // peer requester
        GlobalScope.launch {
            while (isActive) {
                val newPeers = tracker.requestPeers().map { (ip, port) ->
                    PeerConnection(InetSocketAddress(ip, port), input)
                }.toMutableSet()
                newPeers.removeAll(activePeers)

                newPeers.forEach { p ->
                    GlobalScope.launch {
                        p.start(tracker.torrentData.infoSHA1, tracker.torrentData.peerId)
                    }
                    activePeers.add(p)
                }
                delay(1000 * 60)
            }
        }
    }

    private fun isPieceValid(message: Piece): Boolean {
        val expectedHash = tracker.torrentData.getPieceSha1(message.id)
        val pieceHash = sha1(message.bytes)
        return Arrays.equals(expectedHash, pieceHash)
    }

    // returns amount of downloads initiated
    private fun initiateDownloadIfNecessary(
        piecesToPeers: Map<Int, PieceInfo>,
        downloadsInProgress: Int
    ): List<Int> {
        val amount = maxSimultaneousDownloads - downloadsInProgress
        val ignoreDuplicates = piecesToPeers.size <= tracker.torrentData.numPieces * 0.04 // boost speed at the end
        return if (amount == 0) emptyList()
        else
            piecesToPeers
                .map { (_, piece) -> piece }
                .filter { piece ->
                    (ignoreDuplicates || !piece.inProgress) && piece.peers.isNotEmpty()
                }
                .sortedBy { it.peers.size } // rarest first
                .take(amount)
                .mapNotNull { piece ->
                    val peer = piece.peers.random() // TODO get fastest from free peers
                    val offer = try {
                        peer.input.offer(DownloadRequest(piece.id, piece.length))
                    } catch (ex: ClosedSendChannelException) {
                        false
                    }
                    if (offer) {
                        piece.inProgress = true
                        piece.id
                    } else
                        null
                }.toList()
    }

    fun shutdown() {
        runBlocking {
            disk.shutdown()
        }
    }
}