package disk

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import krot.Piece
import main.progress.Progress
import tracker.TorrentData
import utils.getPieceSha1
import utils.intersection
import utils.sha1
import java.io.File
import java.io.RandomAccessFile
import java.util.*
import kotlin.collections.ArrayList

data class FileOperation(val file: List<String>, val offset: Long, val data: ByteArray)

class Disk(private val rootFolder: File, private val torrentData: TorrentData) {
    val input = Channel<Piece>(50)
    private val filesCache = mutableMapOf<String, RandomAccessFile>()
    private var writerJob: Job? = null

    fun checkDownloadedPieces(progress: Progress): Set<Int> {
        val presentPieces = List(torrentData.numPieces) { i ->
            val byteOffset = i * torrentData.pieceLength
            val byteLength =
                if (i == torrentData.numPieces - 1) torrentData.lastPieceLength else torrentData.pieceLength
            val expectedSha1 = torrentData.getPieceSha1(i)
            val filePieceSha1 = getFilePieceSha1(byteOffset, byteLength)

            if (expectedSha1.contentEquals(filePieceSha1)) {
                progress.setDone(i)
                progress.printProgress()
                i
            } else {
                null
            }
        }.filterNotNull()

        closeFiles()
        return presentPieces.toSet()
    }

    private fun getFilePieceSha1(
        byteOffset: Long,
        byteLength: Long
    ): ByteArray {
        val files = torrentData.files
        var fileOffset = 0L
        var pieceBytes = ByteArray(0)
        for (f in files) {
            val file = File(rootFolder, f.path.joinToString(File.separator))
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

    fun initWriter() {
        val pieceLength = torrentData.pieceLength
        val pieceBufferSize = 100 * 1024 * 1024 // 100MB
        val bufferLength = pieceBufferSize / pieceLength
        writerJob = GlobalScope.launch {
            val pieceBuffer = ArrayList<Piece>()
            for (piece in input) {
                pieceBuffer.add(piece)
                if (pieceBuffer.size >= bufferLength) {
                    writeToDisk(pieceBuffer)
                    pieceBuffer.clear()
                }
            }
            writeToDisk(pieceBuffer)
            closeFiles()
        }
    }

    private suspend fun writeToDisk(pieces: List<Piece>) {
        withContext(Dispatchers.IO) {
            for (piece in pieces.sortedBy { it.id }) {
                for ((path, fileOperations) in prepare(piece).groupBy { it.file }) {
                    val raf = openRandomAccessFile(File(rootFolder, path.joinToString(File.separator)))
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

    suspend fun shutdown() {
        input.close()
        writerJob?.join()
        withContext(Dispatchers.IO + NonCancellable) {
            for ((_, f) in filesCache) {
                f.close()
            }
        }
        filesCache.clear()
    }

    private fun closeFiles() {
        for ((_, f) in filesCache) {
            f.close()
        }
        filesCache.clear()
    }

    // TODO Simplify, make readable
    private fun prepare(piece: Piece): List<FileOperation> {
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
                pieceByteOffset,
                pieceByteOffset + pieceByteLength
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
}

