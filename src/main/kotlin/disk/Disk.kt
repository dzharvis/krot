package disk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import main.Piece
import tracker.TorrentData
import java.io.File
import java.io.RandomAccessFile

data class FileOperation(val file: List<String>, val offset: Long, val data: ByteArray)

object Disk {
    fun initWriter(input: Channel<Piece>, torrent: TorrentData) {
        GlobalScope.launch {
            for (piece in input) {
                val fileOperations = prepare(piece, torrent)
                withContext(Dispatchers.IO) {
                    for ((file, offset, data) in fileOperations) {
                        // todo group by file
                        val fileName = "/tmp/${file.joinToString("/")}"
                        val f = File(fileName)
                        f.parentFile.mkdirs()
                        val raf = RandomAccessFile(f, "rw")
                        raf.seek(offset)
                        raf.write(data)
                        raf.close()
                    }
                }
            }

        }
    }

    // shoto slozhno kakto polushilos' no dolzhno rabotat' ))))0
    private fun prepare(piece: Piece, torrentData: TorrentData): List<FileOperation> {
        val (torrent, _, _, _, numPieces, pieceLength, lastPieceLength) = torrentData
        val pieceByteOffset = piece.id * pieceLength
        val pieceByteLength = piece.bytes.size
        var fileOffset = 0L
        return torrent["info"]!!.map["files"]!!.list
            .mapIndexed { i, file ->
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
            }.filterNotNull()
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

