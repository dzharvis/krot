package utils

data class BandwidthBucket(val time: Long, val value: Long)

class BandwidthCalculator(val timeWindow: Long = 5000) {
    private val bandwidthWindow = mutableListOf<BandwidthBucket>()

    // Can potentially grow large in size, but whatever
    fun record(value: Long) {
        val currentTime = System.currentTimeMillis()
        bandwidthWindow.removeIf { it.time < currentTime - timeWindow }
        bandwidthWindow.add(BandwidthBucket(System.currentTimeMillis(), value))
    }

    fun getBandwidth(): Long {
        val currentTime = System.currentTimeMillis()
        return bandwidthWindow.fold(0L, { acc, bucket -> acc + bucket.value }) / 5
    }
}
