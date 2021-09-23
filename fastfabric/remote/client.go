package remote

import (
	"google.golang.org/grpc"
)

var storageClient StoragePeerClient

func StartStoragePeerClient(address string) error {
	// TODO: use stream
	var MAXSIZE = 64 * 1024 * 1024
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MAXSIZE), grpc.MaxCallSendMsgSize(MAXSIZE)))
	if err != nil {
		return err
	}
	storageClient = NewStoragePeerClient(conn)
	return nil
}

func GetStoragePeerClient() StoragePeerClient {
	return storageClient
}
