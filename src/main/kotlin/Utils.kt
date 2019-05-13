package utils

import tracker.TorrentData
import java.security.MessageDigest

data class Intersection(val intersects: Boolean, val x: Long, val y: Long)

fun TorrentData.getPieceSha1(pieceId: Int): ByteArray {
    val sha1Length = 20
    return this.piecesSha1.copyOfRange(pieceId * sha1Length, pieceId * sha1Length + sha1Length)
}

fun sha1(bytes: ByteArray): ByteArray {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(bytes, 0, bytes.size)
    return md.digest()!!
}

fun intersection(x1: Long, y1: Long, x2: Long, y2: Long): Intersection {
    val leftBound = Math.max(x1, x2)
    val rightBound = Math.min(y1, y2)
    val intersects = rightBound - leftBound
    return if (intersects > 0) {
        Intersection(true, leftBound, rightBound)
    } else {
        Intersection(false, -1, -1)
    }
}