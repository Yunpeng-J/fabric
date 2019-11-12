package config

var BlockPipelineWidth = 31
var Gas = 1000000

var IsStorage = false
var IsEndorser = false
var IsBenchmark = false

var StorageAddress = ""
var RegisterBlockStore func(ledgerId string, blockStore interface{})
