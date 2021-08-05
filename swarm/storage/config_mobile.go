
// +build  android ios

package storage
import "github.com/syndtr/goleveldb/leveldb/opt"
// clientIdentifier is a hard coded identifier to report into the network.
const openFileLimit = 256
var fileSizeLimit =  opt.DefaultCompactionTableSize