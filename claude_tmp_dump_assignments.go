package main

import (
	"encoding/json"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

type Assignment struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	OwnerID    string `json:"owner_id"`
	FollowerID string `json:"follower_id"`
}

func main() {
	if len(os.Args) < 2 {
		panic("path required")
	}
	db, err := bolt.Open(os.Args[1], 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_ = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("assignments"))
		if b == nil {
			fmt.Println("no assignments bucket")
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var a Assignment
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			fmt.Printf("%s => %+v\n", string(k), a)
			return nil
		})
	})
}
