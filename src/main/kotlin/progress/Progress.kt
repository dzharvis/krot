package main.progress

sealed class ProgressState
object InProgress: ProgressState()
object Done: ProgressState()
object Empty: ProgressState()

class Progress(private val numPieces: Int, private val pieceSize: Long) {
    private val progress: Array<ProgressState> = Array(numPieces) { Empty }
    private val bandwidthWindow = mutableListOf<Long>()
    var state = "Init"
        set(value) {
            field = value
            printProgress()
        }
    var numPeers = 0
        set(value) {
            field = value
            printProgress()
        }

    fun setDone(id: Int) {
        progress[id] = Done
        recalcBandwidth()
        printProgress()
    }

    fun setEmpty(id: Int) {
        progress[id] = Empty
        printProgress()
    }

    fun setInProgress(id: Int) {
        progress[id] = InProgress
        recalcBandwidth()
        printProgress()
    }

    private fun recalcBandwidth() {
        val currentTime = System.currentTimeMillis()
        bandwidthWindow.add(currentTime)
        while (bandwidthWindow.size > 5000) {
            bandwidthWindow.removeAt(0)
        }
        bandwidthWindow.removeIf { it < currentTime - 5000 }
    }

    fun getProgressString(): String {
        val chunks = numPieces / 50
        return progress.toList().chunked(chunks).joinToString("", "[", "]") { chunk ->
            when {
                chunk.all { it == Done } -> "#"
                chunk.count { it == Done } >= chunks/2 -> "="
                chunk.any { it == InProgress || it == Done } -> "~"
                else -> "-"
            }
        }
    }

    fun getProgressPercent(): String {
        val donePieces = progress.filter { it == Done }.size
        val percent = donePieces * 100 / numPieces
        return "$percent%"
    }

    fun getBandwidth(): String {
        val currentTime = System.currentTimeMillis()
        val bytesReceived = bandwidthWindow.filter { it >= currentTime - 5000 }.fold(0L, { acc, _ -> acc + pieceSize })
        val bps = bytesReceived / 1024 / 5
        return "$bps KB/sec"
    }

    fun printProgress() {
        System.out.print("\r")
        val peersStr = if (state.equals("Downloading")) "from ${numPeers} numPeers" else ""
        System.out.print("$state ${getProgressPercent()} : $peersStr ${getProgressString()} ${getBandwidth()}")
        System.out.flush()
    }

    override fun toString(): String {
        return getProgressString()
    }

}