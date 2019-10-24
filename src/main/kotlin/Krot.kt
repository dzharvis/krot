package krot

import disk.Disk
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.channels.TickerMode
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import main.progress.Progress
import main.progress.State
import protocol.PeerConnection
import tracker.Tracker
import utils.getPieceSha1
import utils.log
import utils.sha1
import java.net.InetSocketAddress
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// messages for communication with peer clients
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
    private val maxSimultaneousDownloads = 100

    fun start() {
        val (pieceLength, _, numPieces) = tracker.torrentData
        val progress = Progress(numPieces, pieceLength)
        progress.printProgress()

        progress.state = State.HASH_CHECK
        val presentPieces = disk.checkDownloadedPieces(progress)
        if (presentPieces.size == numPieces) {
            progress.state = State.DONE
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
            progress.state = State.DOWNLOADING
            val ticker = ticker(delayMillis = 5000, initialDelayMillis = 0, mode = TickerMode.FIXED_DELAY)
            while (true) {
                when (val message = select<SupervisorMsg> {
                    ticker.onReceive { Ticker }
                    input.onReceive { it }
                }) {
                    is HasPiece -> {
                        piecesToPeers[message.id]?.peers?.add(message.peer)
                    }
                    is Ticker -> {
                        val downloads = initiateDownloadIfNecessary(piecesToPeers, downloadsInProgress)
                        log("[Ticker] $downloads, ${downloads.size} downloads has started")
                        downloadsInProgress += downloads.size
                        for (d in downloads) {
                            progress.setInProgress(d)
                        }
                        log("[Ticker] active peers -> ${activePeers.size} $activePeers")
                        log("[Ticker] inflight downloads -> $downloadsInProgress")
                        progress.printProgress()
                    }
                    is Closed -> {
                        for ((_, v) in piecesToPeers) {
                            v.peers.remove(message.peer)
                        }
                        activePeers.remove(message.peer)
                        progress.numPeers = activePeers.size
                        log("[Closed] ${message.peer.addr} died")
                        log("[Closed] active peers -> ${activePeers.size} $activePeers")
                        log("[Closed] inflight downloads -> $downloadsInProgress")
                    }
                    is DownloadCanceledRequest -> {
                        downloadsInProgress--
                        piecesToPeers[message.id]?.inProgress = false
                        log("[DownloadCanceledRequest] ${message.id}")
                        log("[DownloadCanceledRequest] active peers -> ${activePeers.size} $activePeers")
                        log("[DownloadCanceledRequest] inflight downloads -> $downloadsInProgress")
                    }
                    is Piece -> {
                        log("[Piece] ${message.id} arrived")
                        progress.numPeers = activePeers.size
                        downloadsInProgress--
                        val hashEqual = message.isValid()
                        if (!hashEqual) {
                            log("[Piece] ${message.id} hash check failed")
                            piecesToPeers[message.id]?.inProgress = false
                            progress.setEmpty(message.id)
                        } else {
                            log("[Piece] ${message.id} hash success")
                            piecesToPeers.remove(message.id)
                            disk.input.send(message)
                            progress.setDone(message.id)
                            if (piecesToPeers.isEmpty()) {
                                progress.state = State.DONE
                                disk.input.close()
                                return@launch
                            }
                        }
                        val downloads = initiateDownloadIfNecessary(piecesToPeers, downloadsInProgress)
                        log("[Piece] $downloads, ${downloads.size} downloads has started")
                        downloadsInProgress += downloads.size
                        log("[Piece] inflight downloads -> $downloadsInProgress")
                    }
                    else -> error(message)
                }
                progress.downloadsInProgress = downloadsInProgress
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

    private fun Piece.isValid(): Boolean {
        val expectedHash = tracker.torrentData.getPieceSha1(this.id)
        val pieceHash = sha1(this.bytes)
        return Arrays.equals(expectedHash, pieceHash)
    }

    // returns list of piece ids initiated for download
    private fun initiateDownloadIfNecessary(
        piecesToPeers: Map<Int, PieceInfo>,
        downloadsInProgress: Int
    ): List<Int> {
        val amount = maxSimultaneousDownloads - downloadsInProgress
        val ignoreDuplicates = piecesToPeers.size <= tracker.torrentData.numPieces * 0.04 // boost speed at the end
        return if (amount == 0) emptyList()
        else
            piecesToPeers
                .asSequence()
                .map { (_, piece) -> piece }
                .filter { piece ->
                    (ignoreDuplicates || !piece.inProgress) && piece.peers.isNotEmpty()
                }
                    // TODO force sequential mode via parameter
                    // fastest pieces first
                .map { Pair(it, it.peers.map { peer -> peer.getBandwidth() }.sum())}
                .sortedByDescending {(p, s) -> s }
                .map { (p, _) -> p }
                .take(amount)
                .mapNotNull { piece ->
                    piece
                        .peers
                        .asSequence()
                        .map { Pair(it, it.getBandwidth()) }
                        .sortedByDescending {(_, s) -> s }
                        .firstOrNull {(p, s) ->
                            try {
                                p.input.offer(DownloadRequest(piece.id, piece.length))
                            } catch (ex: ClosedSendChannelException) {
                                false
                            }
                        }?.let {
                            log("[Krot] Peer $it started downloading process")
                            piece.inProgress = true
                            piece.id
                        }
                }.toList()
    }

    fun shutdown() {
        runBlocking {
            disk.shutdown()
        }
    }
}
