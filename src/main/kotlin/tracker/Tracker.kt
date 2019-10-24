package tracker

import be.adaxisoft.bencode.BDecoder
import be.adaxisoft.bencode.BEncoder
import utils.log
import utils.sha1
import java.io.ByteArrayOutputStream
import java.io.File
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder
import java.nio.charset.Charset

data class AnnounceResp(
    val failure: String?,
    val warn: String?,
    val interval: Long,
    val minInterval: Long?,
    val trackerId: String?,
    val complete: Long,
    val incomplete: Long
)

data class PeerAddr(val host: String, val port: Int)
data class TorrentFile(val length: Long, val path: List<String>)

data class TorrentData(
    val pieceLength: Long,
    val lastPieceLength: Long,
    val numPieces: Int,
    val private: Boolean,

    val folder: String?,
    val files: List<TorrentFile>,

    val infoSHA1: ByteArray,
    val peerId: ByteArray,
    val piecesSha1: ByteArray
)

private fun generatePeerId(): ByteArray {
    return sha1("123".toByteArray()) // TODO generate normal peer id
}

private fun infoDictSHA1(info: Any): ByteArray {
    val out = ByteArrayOutputStream()
    BEncoder.encode(info, out)
    return sha1(out.toByteArray())
}

private fun parsePeers(peers: ByteArray): List<PeerAddr> {
    return (peers.indices step 6).map { i ->
        val ip1 = (peers[i] + 0) and 0xff // + 0 for int casting. I don't like .toInt()
        val ip2 = (peers[i + 1] + 0) and 0xff
        val ip3 = (peers[i + 2] + 0) and 0xff
        val ip4 = (peers[i + 3] + 0) and 0xff
        val ip = "$ip1.$ip2.$ip3.$ip4"
        val port = (((peers[i + 4] + 0) and 0xff) shl 8) or ((peers[i + 5] + 0) and 0xff)
        PeerAddr(ip, port)
    }
}

class Tracker private constructor(
    val torrentData: TorrentData,
    val infoHash: String,
    val peerId: String,
    val announceUrl: String
) {
    companion object {
        fun fromFile(file: String): Tracker {
            val torrent = BDecoder(File(file).inputStream()).decodeMap().map
            val torrentInfo = torrent["info"]!!
            val infoSHA1 = infoDictSHA1(torrentInfo)
            val peerId = generatePeerId()
            val private = torrentInfo.map["private"]?.int == 1
            val numPieces = torrentInfo.map["pieces"]!!.bytes.size / 20
            val pieceLength = torrentInfo.map["piece length"]!!.long
            val singleFileMode = torrentInfo.map["files"] == null
            val folderName = if (singleFileMode) null else torrentInfo.map["name"]!!.string
            val piecesSha1 = torrentInfo.map["pieces"]!!.bytes
            val files = if (singleFileMode) {
                val fileName = torrentInfo.map["name"]!!.string
                val fileLength = torrentInfo.map["length"]!!.long
                listOf(TorrentFile(fileLength, listOf(fileName)))
            } else {
                torrentInfo.map["files"]!!.list.map { f ->
                    TorrentFile(
                        f.map["length"]!!.long,
                        f.map["path"]!!.list.map { it.string })
                }
            }
            val numBytes = if (singleFileMode) {
                torrentInfo.map["length"]!!.long
            } else {
                torrentInfo.map["files"]!!.list.map { it.map["length"]!!.long }.fold(0L, { acc, i -> acc + i })
            }
            val diff = numPieces * pieceLength - numBytes
            val charset = Charset.forName("Windows-1251")
            val torrentData = TorrentData(
                pieceLength,
                pieceLength - diff,
                numPieces,
                private,
                folderName,
                files,
                infoSHA1,
                peerId,
                piecesSha1
            )
            return Tracker(
                torrentData,
                String(infoSHA1, charset),
                String(peerId, charset),
                torrent["announce"]!!.string
            )
        }
    }

    var trackerId: String? = null

    fun requestPeers(): List<PeerAddr> {
        val paramsList = mutableListOf(
            "info_hash" to infoHash,
            "uploaded" to "0",
            "downloaded" to "0",
            "port" to "10000",
            "compact" to "1",
            "numwant" to "100",
            "peer_id" to peerId,
            "left" to "10001"
        )
        if (trackerId != null) {
            paramsList.add("trackerid" to trackerId!!)
        }

        val params = paramsList.joinToString(separator = "&") { (k, v) -> "$k=${URLEncoder.encode(v, "Windows-1251")}" }

        val query = URL("$announceUrl&$params")
        val conn = query.openConnection() as HttpURLConnection
        return try {
            conn.requestMethod = "GET"
            val response = BDecoder(conn.inputStream.buffered()).decodeMap().map
            if (trackerId == null) trackerId = response["tracker id"]?.string
            require(response["failure reason"] == null) { response["failure reason"]!!.string }
            parsePeers(response["peers"]!!.bytes)
        } catch (ex: Exception) {
            log(ex.toString())
            emptyList()
        } finally {
            conn.disconnect()
        }
    }
}
