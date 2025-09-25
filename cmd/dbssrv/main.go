// Copyright © 2024 FORTH-ICS
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NBD server for DBS.
package main

import (
	"fmt"
	"net"
	"os"
	"sync"

	nbd "github.com/chazapis/go-nbd/pkg/server"
	"github.com/jawher/mow.cli"
	"golang.org/x/exp/slices"

	"github.com/Kampadais/dbs"
)

type NbdBackend struct {
	sync.RWMutex
	vc   *dbs.VolumeContext
	size uint64
}

func NewNbdBackend(vc *dbs.VolumeContext, size uint64) *NbdBackend {
	return &NbdBackend{
		vc:   vc,
		size: size,
	}
}

func (b *NbdBackend) ReadAt(p []byte, off int64) (n int, err error) {
	b.RLock()
	defer b.RUnlock()
	return len(p), b.vc.ReadAt(p, uint64(off))
}

func (b *NbdBackend) WriteAt(p []byte, off int64) (n int, err error) {
	if b.vc == nil {
		return 0, fmt.Errorf("replica no longer exists")
	}

	// Try with a read lock and upgrade to a write lock if necessary
	b.RLock()
	err = b.vc.WriteAt(p, uint64(off), false)
	if err != nil {
		b.RUnlock()
		b.Lock()
		err := b.vc.WriteAt(p, uint64(off), true)
		if err != nil {
			return 0, err
		}
		b.Unlock()
		return len(p), nil
	}
	b.RUnlock()
	return len(p), err
}

func (b *NbdBackend) Size() (int64, error) {
	return int64(b.size), nil
}

func (b *NbdBackend) Sync() error {
	return nil
}

func startServer(url *string, device *string, volumeName *string) error {
	volumeInfo, err := dbs.GetVolumeInfo(*device)
	if err != nil {
		return err
	}
	volumeIdx := slices.IndexFunc(volumeInfo, func(vi dbs.VolumeInfo) bool { return vi.VolumeName == *volumeName })
	if volumeIdx == -1 {
		return fmt.Errorf("volume %v not found", volumeName)
	}
	vc, err := dbs.OpenVolume(*device, *volumeName)
	if err != nil {
		return err
	}
	backend := NewNbdBackend(vc, volumeInfo[volumeIdx].VolumeSize)

	listener, err := net.Listen("tcp", *url)
	if err != nil {
		return err
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		fmt.Printf("New connection from: %v\n", conn.RemoteAddr())
		go func() {
			defer conn.Close()

			if err := nbd.Handle(
				conn,
				[]*nbd.Export{
					{
						Name:        "",
						Description: "DBS",
						Backend:     backend,
					},
				},
				&nbd.Options{
					ReadOnly:           false,
					MinimumBlockSize:   dbs.BLOCK_SIZE,
					PreferredBlockSize: dbs.BLOCK_SIZE,
					MaximumBlockSize:   dbs.BLOCK_SIZE,
				}); err != nil {
				fmt.Printf("Failed to handle nbd connection: %v\n", err)
			}
		}()
	}
}

func main() {
	app := cli.App("dbssrv", "NBD server for DBS")
	url := app.StringOpt("u url", "localhost:10809", "Server URL")
	device := app.StringArg("DEVICE", "", "")
	volume := app.StringArg("VOLUME", "", "")
	app.Action = func() {
		if err := startServer(url, device, volume); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
	}
	app.Run(os.Args)
}
