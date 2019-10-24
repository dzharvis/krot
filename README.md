![Usage example](pics/mole.png)
![](https://github.com/dzharvis/krot/workflows/Build%20and%20Release/badge.svg)
# Command Line Torrent Client
Command line torrent client written in kotlin using coroutines.

## Usage
`java -jar  krot.jar ~/Downloads/lego.torrent /tmp/lego/`

First argument is torrent file, second - destination folder.

![Usage example](pics/screenshot.png)

## Todo
- Windows cmd refresh fix
- Multithreaded hash check at init
- Magnet links
- Write tests
- DHT + peers boost
- Track fastest peers
- Start simultaneously with hash check
- Error with 0day tracker
	java.lang.IllegalArgumentException: Torrent not registered with this tracker
- Netty
- Investigate high CPU
    - There are other java based clients - compare
- TCP/UDP
- Native image
- Prettify cmd arguments
- Implement faster sha1
- Multithreaded sha1 for peers
