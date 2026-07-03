// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongoreplay

import (
	"fmt"

	"github.com/google/gopacket/pcap"
)

// ListInterfacesCommand stores settings for the mongoreplay 'list-interfaces' subcommand
type ListInterfacesCommand struct {
	GlobalOpts *Options `no-flag:"true"`
}

// Execute runs the program for the 'list-interfaces' subcommand
func (lic *ListInterfacesCommand) Execute(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("unknown argument: %s", args[0])
	}

	devices, err := pcap.FindAllDevs()
	if err != nil {
		return fmt.Errorf("error listing network interfaces: %v", err)
	}

	for _, device := range devices {
		fmt.Printf("%s\n", device.Name)
		if device.Description != "" {
			fmt.Printf("\tDescription: %s\n", device.Description)
		}
		for _, address := range device.Addresses {
			fmt.Printf("\tAddress: %s\n", address.IP)
		}
	}
	return nil
}
