package main

import (
	"os"
	"fmt"
	virtruclient "github.com/virtru/go-tdf3-sdk-wrapper"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	virtruSDK := virtruclient.NewVirtruClient(os.Getenv("TDF_USER"), os.Getenv("TDF_APPID"), logger)
	policy := virtruclient.VirtruPolicy{EnableReshare: false, UsersWithAccess: []string{"dan.virtru@gmail.com"}}
        res, _ := virtruSDK.EncryptString("holla at ya boi", false, &policy)
        logger.Sugar().Debugf("Got TDF encrypted payload %s", string(res))
	fmt.Printf("tdf: %x", res)
	fmt.Printf("Hi\n")
	virtruSDK.Close()
}
