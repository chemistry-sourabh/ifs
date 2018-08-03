package main

import (
	_ "go.uber.org/automaxprocs"
	"gopkg.in/urfave/cli.v1"
	"os"
	"fmt"
	"ifs"
)

//TODO Remove Logs for automaxprocs
func main() {

	app := cli.NewApp()
	app.EnableBashCompletion = true
	app.Version = "0.1.0"
	app.Name = "ifs"
	app.HelpName = "ifs"
	app.Usage = "A Fast Network File System that can Mount Paths from Multiple Hosts"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "c, config",
			Usage: "Specify the Config File",
			Value: "./fs.json",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "mount",
			Aliases: []string{"mnt"},
			Usage:   "Mount the Filesystem",
			Action: func(c *cli.Context) error {
				cfg := &ifs.FsConfig{}
				path := c.GlobalString("config")
				err := cfg.Load(path)

				if err != nil {
					return err
				}

				ifs.SetupLogger(cfg.Log)
				ifs.MountRemoteRoots(cfg)
				return nil
			},
		},
		{
			Name:    "umount",
			Aliases: []string{"umnt"},
			Usage:   "Unmount the Filesystem",
			Action: func(c *cli.Context) error {
				fmt.Println("Unimplemented")
				return nil
			},
		},
		{
			Name:  "add",
			Usage: "Add a New Path to Mount",
			Action: func(c *cli.Context) error {
				fmt.Println("Unimplemented")
				return nil
			},
		},
		{
			Name: "remove",
			Aliases: []string{"rm"},
			Usage: "Remove a Mounted Path",
			Action: func(c *cli.Context) error {
				fmt.Println("Unimplemented")
				return nil
			},
		},
		{
			Name:    "list",
			Aliases: []string{"ls"},
			Usage:   "List Mounted Paths",
			Action: func(c *cli.Context) error {
				fmt.Println("Unimplemented")
				return nil
			},
		},
	}

	err := app.Run(os.Args)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
