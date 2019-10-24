package main.progress

import utils.BandwidthCalculator
import utils.progress

sealed class ProgressState
object InProgress: ProgressState()
object Done: ProgressState()
object Empty: ProgressState()

enum class State(val label: String) {
    INIT("Init"),
    DOWNLOADING("Downloading"),
    HASH_CHECK("Hash check"),
    DONE("Done")
}

class Progress(private val numPieces: Int, private val pieceSize: Long) {
    private val progress: MutableList<ProgressState> = MutableList(numPieces) { Empty }

    private val bandwidth = BandwidthCalculator()

    var state = State.INIT
    var numPeers = 0
    var downloadsInProgress = 0

    fun setDone(id: Int) {
        progress[id] = Done
        bandwidth.record(pieceSize)
    }

    fun setEmpty(id: Int) {
        progress[id] = Empty
    }

    fun setInProgress(id: Int) {
        progress[id] = InProgress
    }

    private fun getProgressString(): String {
        val chunks = numPieces / 50
        return progress.chunked(chunks).joinToString("", "[", "]") { chunk ->
            when {
                chunk.all { it == Done } -> "#"
                chunk.count { it == Done } >= chunks/2 -> "="
                chunk.any { it == InProgress || it == Done } -> "~"
                else -> "-"
            }
        }
    }

    private fun getProgressPercent(): String {
        val donePieces = progress.filter { it == Done }.size
        val percent = donePieces * 100 / numPieces
        return "$percent%"
    }

    private fun getBandwidth(): String {
        val bps = bandwidth.getBandwidth() / 1024 / 5
        return "$bps KB/sec"
    }

    var maxProgressLen = 0

    fun printProgress() {
        val peersStr = if (state == State.DOWNLOADING) "from $downloadsInProgress($numPeers) peers" else ""
        var progressStr = "${state.label} ${getProgressPercent()} : $peersStr ${getProgressString()} ${getBandwidth()}"
        // pad with spaces in order to clear old values
        progressStr = if (progressStr.length < maxProgressLen) {
            val diff = maxProgressLen - progressStr.length
            val spaces = (0 until diff).joinToString("") {" "}
            "$progressStr$spaces"
        } else {
            maxProgressLen = progressStr.length
            progressStr
        }
        progress("\r$progressStr")
    }

    override fun toString(): String {
        return getProgressString()
    }

}
