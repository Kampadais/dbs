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

// A command line utility exposing the query and management APIs of DBS.
package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/jawher/mow.cli"
	"github.com/jedib0t/go-pretty/v6/table"

	"github.com/Kampadais/dbs"
)

var device *string

func cmdGetDeviceInfo(cmd *cli.Cmd) {
	cmd.Action = func() {
		di, err := dbs.GetDeviceInfo(*device)
		if err != nil {
			fmt.Println(err)
			return
		}

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendRows([]table.Row{
			{"version", di.Version},
			{"device_size", units.HumanSize(float64(di.DeviceSize))},
			{"total_device_extents", di.TotalDeviceExtents},
			{"allocated_device_extents", di.AllocatedDeviceExtents},
			{"volume_count", di.VolumeCount},
		})
		t.Render()
	}
}

func cmdGetVolumeInfo(cmd *cli.Cmd) {
	cmd.Action = func() {
		vi, err := dbs.GetVolumeInfo(*device)
		if err != nil {
			fmt.Println(err)
			return
		}

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendRow(table.Row{"volume_name", "volume_size", "created_at", "snapshot_id", "snapshot_count"})
		t.AppendSeparator()
		for i := range vi {
			t.AppendRow(table.Row{
				vi[i].VolumeName,
				units.HumanSize(float64(vi[i].VolumeSize)),
				vi[i].CreatedAt,
				vi[i].SnapshotId,
				vi[i].SnapshotCount,
			})
		}
		t.Render()
	}
}

func cmdGetSnapshotInfo(cmd *cli.Cmd) {
	volumeName := cmd.StringArg("VOLUME_NAME", "", "")
	cmd.Action = func() {
		si, err := dbs.GetSnapshotInfo(*device, *volumeName)
		if err != nil {
			fmt.Println(err)
			return
		}

		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendRow(table.Row{"snapshot_id", "parent_snapshot_id", "created_at", "labels"})
		t.AppendSeparator()
		for i := range si {
			psid := strconv.Itoa(int(si[i].ParentSnapshotId))
			if psid == "0" {
				psid = "-"
			}
			var labelStr string
			if len(si[i].Labels) == 0 {
				labelStr = "-" // no labels
			} else {
				pairs := make([]string, 0, len(si[i].Labels))
				for k, v := range si[i].Labels {
					pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
				}
				sort.Strings(pairs) // optional, for stable output
				labelStr = strings.Join(pairs, ", ")
			}

			t.AppendRow(table.Row{
				si[i].SnapshotId,
				psid,
				si[i].CreatedAt,
				labelStr,
			})
		}
		t.Render()
	}
}

func cmdInitDevice(cmd *cli.Cmd) {
	cmd.Action = func() {
		if err := dbs.InitDevice(*device); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdVacuumDevice(cmd *cli.Cmd) {
	cmd.Action = func() {
		if err := dbs.VacuumDevice(*device); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdCreateVolume(cmd *cli.Cmd) {
	volumeName := cmd.StringArg("VOLUME_NAME", "", "")
	volumeSize := cmd.StringArg("VOLUME_SIZE", "", "")
	cmd.Action = func() {
		bytesSize, err := units.FromHumanSize(*volumeSize)
		if err != nil {
			fmt.Println(err)
			return
		}
		if err := dbs.CreateVolume(*device, *volumeName, uint64(bytesSize)); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdRenameVolume(cmd *cli.Cmd) {
	volumeName := cmd.StringArg("VOLUME_NAME", "", "")
	newVolumeName := cmd.StringArg("NEW_VOLUME_NAME", "", "")
	cmd.Action = func() {
		if err := dbs.RenameVolume(*device, *volumeName, *newVolumeName); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdCreateSnapshot(cmd *cli.Cmd) {
	cmd.Spec = "VOLUME_NAME [LABELS...]"
	volumeName := cmd.StringArg("VOLUME_NAME", "", "")
	labelArgs := cmd.StringsArg("LABELS", nil, "Labels to attach to the snapshot in key=value format")

	cmd.Action = func() {
		labels := make(map[string]string)

		// Only parse if user supplied labels
		if labelArgs != nil {
			for _, arg := range *labelArgs {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					fmt.Printf("invalid label format: %q (expected key=value)\n", arg)
					os.Exit(1)
				}
				key := strings.TrimSpace(parts[0])
				val := strings.TrimSpace(parts[1])
				labels[key] = val
			}
		}

		if err := dbs.CreateSnapshot(*device, *volumeName, true, time.Now().Format(time.RFC3339), labels); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdCloneSnapshot(cmd *cli.Cmd) {
	newVolumeName := cmd.StringArg("NEW_VOLUME_NAME", "", "")
	snapshotId := cmd.IntArg("SNAPSHOT_ID", 0, "")
	cmd.Action = func() {
		if err := dbs.CloneSnapshot(*device, *newVolumeName, uint(*snapshotId)); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdDeleteVolume(cmd *cli.Cmd) {
	volumeName := cmd.StringArg("VOLUME_NAME", "", "")
	cmd.Action = func() {
		if err := dbs.DeleteVolume(*device, *volumeName); err != nil {
			fmt.Println(err)
		}
	}
}

func cmdDeleteSnapshot(cmd *cli.Cmd) {
	snapshotId := cmd.IntArg("SNAPSHOT_ID", 0, "")
	cmd.Action = func() {
		if err := dbs.DeleteSnapshot(*device, uint(*snapshotId)); err != nil {
			fmt.Println(err)
		}
	}
}

func main() {
	app := cli.App("dbsctl", "DBS command line tool")
	device = app.StringArg("DEVICE", "", "")
	app.Command("get_device_info", "", cmdGetDeviceInfo)
	app.Command("get_volume_info", "", cmdGetVolumeInfo)
	app.Command("get_snapshot_info", "", cmdGetSnapshotInfo)
	app.Command("init_device", "", cmdInitDevice)
	app.Command("vacuum_device", "", cmdVacuumDevice)
	app.Command("create_volume", "", cmdCreateVolume)
	app.Command("rename_volume", "", cmdRenameVolume)
	app.Command("create_snapshot", "", cmdCreateSnapshot)
	app.Command("clone_snapshot", "", cmdCloneSnapshot)
	app.Command("delete_volume", "", cmdDeleteVolume)
	app.Command("delete_snapshot", "", cmdDeleteSnapshot)
	app.Run(os.Args)
}
