package tracker

import be.adaxisoft.bencode.BDecoder
import be.adaxisoft.bencode.BEncodedValue
import be.adaxisoft.bencode.BEncoder
import utils.sha1
import java.io.ByteArrayOutputStream
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.Charset

fun generatePeerId(): ByteArray {
    return sha1("123".toByteArray()) // TODO generate normal peer id
}

fun infoDictSHA1(info: Any): ByteArray {
    val out = ByteArrayOutputStream()
    BEncoder.encode(info, out)
    return sha1(out.toByteArray())
}

fun parsePeers(peers: ByteArray): List<PeerAddr> {
    return (0 until peers.size step 6).map { i ->
        val ip1 = (peers[i] + 0) and 0xff // + 0 for int casting. I don't like .toInt()
        val ip2 = (peers[i + 1] + 0) and 0xff
        val ip3 = (peers[i + 2] + 0) and 0xff
        val ip4 = (peers[i + 3] + 0) and 0xff
        val ip = "$ip1.$ip2.$ip3.$ip4"
        val port = (((peers[i + 4] + 0) and 0xff) shl 8) or ((peers[i + 5] + 0) and 0xff)
        PeerAddr(ip, port)
    }
}

private fun getPeers(infoHash: String, peerIdHash: String, url: String): List<PeerAddr> {
    val params = mapOf(
        "info_hash" to infoHash,
        "uploaded" to "0",
        "downloaded" to "0",
        "port" to "10000",
        "compact" to "1",
        "numwant" to "100",
        "peer_id" to peerIdHash,
        "left" to "10001"
    )
        .map { (k, v) -> "$k=${URLEncoder.encode(v, "Windows-1251")}" }
        .joinToString(separator = "&")

    val query = URL("$url&$params")
    val conn = query.openConnection() as HttpURLConnection
    try {
        conn.requestMethod = "GET"
        val response = BDecoder(conn.inputStream.buffered()).decodeMap().map
        return parsePeers(response["peers"]!!.bytes)
    } finally {
        conn.disconnect()
    }
}

data class PeerAddr(val host: String, val port: Int)
data class TorrentData(
    val torrent: Map<String, BEncodedValue>,
    val peers: List<PeerAddr>,
    val sha1: ByteArray,
    val peerId: ByteArray,
    val numPieces: Int,
    val pieceLength: Long,
    val lastPieceLength: Long
)

fun processFile(file: String): TorrentData {
    val charset = Charset.forName("Windows-1251")
    val torrent = BDecoder(File(file).inputStream()).decodeMap().map
    val sha1 = getSHA1(torrent["info"]!!)
    val peerId = generatePeerId()
    val numPieces = torrent["info"]!!.map["pieces"]!!.bytes.size / 20
    val pieceLength = torrent["info"]!!.map["piece length"]!!.long
    val peers = getPeers(String(sha1, charset), String(peerId, charset), torrent["announce"]!!.string)
    val numBytes = torrent["info"]!!.map["files"]!!.list.map { it.map["length"]!!.long }.fold(0L, { acc, i -> acc + i })
    val diff = numPieces * pieceLength - numBytes
    return TorrentData(torrent, peers, sha1, peerId, numPieces, pieceLength, pieceLength - diff)
}