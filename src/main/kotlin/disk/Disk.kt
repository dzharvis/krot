package disk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import main.Piece
import main.progress.Progress
import tracker.TorrentData
import utils.sha1
import java.io.File
import java.io.RandomAccessFile
import java.util.*
import kotlin.collections.ArrayList

data class FileOperation(val file: List<String>, val offset: Long, val data: ByteArray)

object Disk {
    private val filesCache = mutableMapOf<String, RandomAccessFile>()

    fun checkDownloadedPieces(torrentData: TorrentData, folder: String, progress: Progress): List<Int> {
        val presentPieces = List(torrentData.numPieces) { i ->
            val byteOffset = i * torrentData.pieceLength
            val byteLength =
                if (i == torrentData.numPieces - 1) torrentData.lastPieceLength else torrentData.pieceLength
            val expectedSha1 = torrentData.piecesSha1.copyOfRange(i * 20, i * 20 + 20)
            val filePieceSha1 = getFilePieceSha1(i, torrentData, byteOffset, byteLength, folder)

            if (Arrays.equals(expectedSha1, filePieceSha1)) {
                progress.setDone(i)
                progress.printProgress()
                i
            } else {
                null
            }
        }.filterNotNull()

        close()
        filesCache.clear()

        return presentPieces
    }

    private fun getFilePieceSha1(
        pieceId: Int,
        torrentData: TorrentData,
        byteOffset: Long,
        byteLength: Long,
        folder: String
    ): ByteArray {
        val files = torrentData.files
        var fileOffset = 0L
        var pieceBytes = ByteArray(0)
        for (f in files) {
            val rootFolder = File(folder)
            val torrentFolder = torrentData.folder?.let { File(rootFolder, it) } ?: rootFolder
            val file = File(torrentFolder, f.path.joinToString(File.separator))
            val fileLength = f.length
            if (!file.exists()) {
                fileOffset += fileLength
                continue
            }

            val intersection = intersection(
                fileOffset,
                fileOffset + fileLength,
                byteOffset,
                byteOffset + byteLength
            )

            if (!intersection.intersects) {
                fileOffset += fileLength
                continue
            }

//            println("intersection found $pieceId ${intersection.x} ${intersection.y} ${file.absolutePath}")

            val raf = openRandomAccessFile(file)
            val fileBytes = ByteArray((intersection.y - intersection.x).toInt())
            raf.seek(intersection.x - fileOffset)
            val bytesRead = raf.read(fileBytes)
            if (bytesRead != fileBytes.size) return pieceBytes

            pieceBytes += fileBytes
            fileOffset += fileLength

            if (pieceBytes.size == byteLength.toInt()) {
                break
            }
        }
        return sha1(pieceBytes)
    }

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
                    val rootFolder = File(folder)
                    val torrentFolder = torrent.folder?.let { File(rootFolder, it) } ?: rootFolder
                    val f = File(torrentFolder, path.joinToString(File.separator))
                    val raf = openRandomAccessFile(f)
                    for ((_, offset, data) in fileOperations) {
                        raf.seek(offset)
                        raf.write(data)
                    }
                }
            }
        }
    }

    private fun openRandomAccessFile(f: File): RandomAccessFile {
        return if (!filesCache.contains(f.absolutePath)) {
            f.parentFile.mkdirs()
            val file = RandomAccessFile(f, "rw")
            filesCache[f.absolutePath] = file
            file
        } else {
            filesCache[f.absolutePath]!!
        }
    }

    fun close() {
        for ((_, f) in filesCache) {
            f.close()
        }
    }

    // TODO Simplify, make readable
    private fun prepare(piece: Piece, torrentData: TorrentData): List<FileOperation> {
        val pieceLength = torrentData.pieceLength
        val pieceByteOffset = piece.id * pieceLength
        val pieceByteLength = piece.bytes.size
        var fileOffset = 0L
        return torrentData.files.mapNotNull { file ->
            val fileByteOffset = fileOffset
            fileOffset += file.length

            val intersection = intersection(
                fileByteOffset,
                fileByteOffset + file.length,
                pieceByteOffset.toLong(),
                pieceByteOffset + pieceByteLength.toLong()
            )

            if (intersection.intersects) {
                FileOperation(
                    file.path,
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

