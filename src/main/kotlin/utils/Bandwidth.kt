package utils

data class BandwidthBucket(val time: Long, val value: Long)

class BandwidthCalculator(private val timeWindow: Long = 5000) {
    private val bandwidthWindow = ArrayList<BandwidthBucket>()

    @Synchronized
    fun record(value: Long) {
        val currentTimeMillis = System.currentTimeMillis()
        bandwidthWindow.removeIf { it.time < (currentTimeMillis - timeWindow) }
        bandwidthWindow.add(BandwidthBucket(System.currentTimeMillis(), value))
    }

    @Synchronized
    fun getBandwidth(): Long {
        if (bandwidthWindow.isEmpty()) return 0L
        val currentTimeMillis = System.currentTimeMillis()
        val windowMs = (currentTimeMillis - bandwidthWindow[0].time).coerceAtLeast(1)
        val bytesReceived = bandwidthWindow.fold(0L, { acc, bucket -> acc + bucket.value })

        return (bytesReceived / (windowMs / 1000.0)).toLong()
    }
}
