package disk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import main.Piece
import tracker.TorrentData
import java.io.File
import java.io.RandomAccessFile

data class FileOperation(val file: List<String>, val offset: Long, val data: ByteArray)

object Disk {
    private val filesCache = mutableMapOf<String, RandomAccessFile>()

    fun initWriter(input: Channel<Piece>, torrent: TorrentData, folder: String): Job {
        val pieceLength = torrent.pieceLength
        val pieceBufferSize = 100 * 1024 * 1024 // 100MB
        val bufferLength = pieceBufferSize / pieceLength
        return GlobalScope.launch {
            val pieceBuffer = ArrayList<Piece>()
            for (piece in input) {
                pieceBuffer.add(piece)
                if (pieceBuffer.size > bufferLength) {
                    writeToDisk(pieceBuffer, torrent, folder)
                    pieceBuffer.clear()
                }
            }
            writeToDisk(pieceBuffer, torrent, folder)
        }
    }

    private suspend fun writeToDisk(
        pieces: List<Piece>,
        torrent: TorrentData,
        folder: String
    ) {
        for (piece in pieces.sortedBy { it.id }) {
            val fileOperations = prepare(piece, torrent).groupBy { it.file }
            withContext(Dispatchers.IO) {
                for ((path, fileOperations) in fileOperations) {
                    val parent = File(folder)
                    val f = File(parent, path.joinToString("/"))
                    val raf = if (!filesCache.contains(f.absolutePath)) {
                        f.parentFile.mkdirs()
                        val file = RandomAccessFile(f, "rw")
                        filesCache[f.absolutePath] = file
                        file
                    } else {
                        filesCache[f.absolutePath]!!
                    }
                    for ((_, offset, data) in fileOperations) {
                        raf.seek(offset)
                        raf.write(data)
                    }
                }
            }
        }
    }

    fun close() {
        for ((_, f) in filesCache) {
            f.close()
        }
    }

    // TODO Simplify, make readable
    private fun prepare(piece: Piece, torrentData: TorrentData): List<FileOperation> {
        val torrent = torrentData.torrent
        val pieceLength = torrentData.pieceLength
        val pieceByteOffset = piece.id * pieceLength
        val pieceByteLength = piece.bytes.size
        var fileOffset = 0L
        return torrent["info"]!!.map["files"]!!.list.mapNotNull { file ->
            val filePath = file.map["path"]!!.list.map { it.string }
            val fileByteOffset = fileOffset
            val fileLength = file.map["length"]!!.long
            fileOffset += fileLength

            val intersection = intersection(
                fileByteOffset,
                fileByteOffset + fileLength,
                pieceByteOffset.toLong(),
                pieceByteOffset + pieceByteLength.toLong()
            )

            if (intersection.intersects) {
                FileOperation(
                    filePath,
                    intersection.x - fileByteOffset,
                    piece.bytes.copyOfRange(
                        (intersection.x - pieceByteOffset).toInt(),
                        (intersection.y - pieceByteOffset).toInt()
                    )
                )
            } else {
                null
            }
        }
    }

    data class Intersection(val intersects: Boolean, val x: Long, val y: Long)

    private fun intersection(x1: Long, y1: Long, x2: Long, y2: Long): Intersection {
        val intersects = Math.min(y1, y2) - Math.max(x1, x2)
        return if (intersects > 0) {
            Intersection(true, Math.max(x1, x2), Math.min(y1, y2))
        } else {
            Intersection(false, -1, -1)
        }
    }
}

