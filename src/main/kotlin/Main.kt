package main

import async.AsyncNettyClient
import disk.Disk
import io.netty.buffer.PooledByteBufAllocator
import krot.Krot
import tracker.Tracker
import java.io.File
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress


fun main(args: Array<String>) {
    if (args.size < 2) {
        error("Provide torrent file location as first argument and destination folder as a second one")
    }

    val torrentFile = args[0]
    val rootFolder = args[1]

    val tracker = Tracker.fromFile(torrentFile)
    // multi file mode vs single file mode
    // in single file mode - one file is written to the root folder
    // in multi file mode - multiple files are written to a folder with a name
    val workingFolder = tracker.torrentData.folder?.let { File(rootFolder, it) } ?: File(rootFolder)
    val krot = Krot(Disk(workingFolder, tracker.torrentData), tracker)

    Runtime.getRuntime().addShutdownHook(Thread(krot::shutdown))
    krot.start()
}