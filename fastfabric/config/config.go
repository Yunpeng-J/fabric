package config

var Gas = 1000000

var IsStorage = false
var IsEndorser = false
var IsBenchmark = false

var RegisterBlockStore func(ledgerId string, blockStore interface{})
