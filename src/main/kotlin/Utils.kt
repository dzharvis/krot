package utils

import java.security.MessageDigest

fun sha1(bytes: ByteArray): ByteArray {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(bytes, 0, bytes.size)
    return md.digest()!!
}