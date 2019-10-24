package main.progress

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
    private val bandwidthWindow = mutableListOf<Long>()
    var state = State.INIT
    var numPeers = 0
    var downloadsInProgress = 0

    fun setDone(id: Int) {
        progress[id] = Done
        recalcBandwidth()
    }

    fun setEmpty(id: Int) {
        progress[id] = Empty
    }

    fun setInProgress(id: Int) {
        progress[id] = InProgress
    }

    private fun recalcBandwidth() {
        val currentTime = System.currentTimeMillis()
        bandwidthWindow.add(currentTime)
        bandwidthWindow.removeIf { it < currentTime - 5000 } // last 5 sec window
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
        val currentTime = System.currentTimeMillis()
        val bytesReceived = bandwidthWindow.filter { it >= currentTime - 5000 }.fold(0L, { acc, _ -> acc + pieceSize })
        val bps = bytesReceived / 1024 / 5
        return "$bps KB/sec"
    }

    var maxProgressLen = 0

    fun printProgress() {
        val peersStr = if (state == State.DOWNLOADING) "from $downloadsInProgress($numPeers) peers" else ""
        var progressStr = "$state ${getProgressPercent()} : $peersStr ${getProgressString()} ${getBandwidth()}"
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
