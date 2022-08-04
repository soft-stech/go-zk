package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/zk"
)

func main() {
	c, events, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	go func() {
		for e := range events {
			log.Printf("SessionEvent: %+v", e)
		}
		log.Printf("SessionEvent closed")
	}()

	w, err := zk.NewCachedLeavesWalker(c, "/leaves")
	if err != nil {
		panic(err)
	}

	for {
		<-time.After(time.Second)
		leaves := []string{}
		err := w.WalkLeaves(func(p string) error {
			leaves = append(leaves, p)
			return nil
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("Got %d leaves:\n%+v\n", len(leaves), leaves)
	}
}
