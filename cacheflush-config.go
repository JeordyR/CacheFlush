package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type cacheflushConfig struct {
	// Log Settings
	LogFile      string `yaml:"LogFile"`
	DebugLogging bool   `yaml:"DebugLogging"`

	// Permission Settings
	OwnerUID uint32 `yaml:"OwnerUID"`
	OwnerGID uint32 `yaml:"OwnerGID"`

	// Path Settings
	BackingPool         string   `yaml:"BackingPool"`
	CacheDrives         []string `yaml:"CacheDrives"`
	OverrideDirectories []string `yaml:"OverrideDirectories"`

	// Behavior Settings
	ForceFreeSpace         string `yaml:"ForceFreeSpace"`
	MinimumAge             string `yaml:"MinimumAge"`
	CurrentAccessThreshold string `yaml:"CurrentAccessThreshold"`
	FlushPolicy            string `yaml:"FlushPolicy"`
	ClearEmptyDirs         bool   `yaml:"ClearEmptyDirs"`
	SkipMove               bool   `yaml:"SkipMove,omitempty"`
	Force                  bool   `yaml:"Force,omitempty"`

	// Pushover Settings
	PushoverEnabled bool   `yaml:"PushoverEnabled,omitempty"`
	PushoverAppKey  string `yaml:"PushoverAppKey,omitempty"`
	PushoverUserKey string `yaml:"PushoverUserKey,omitempty"`
}

func (conf *cacheflushConfig) isValidPolicy() bool {
	switch conf.FlushPolicy {
	case
		"oldest-first",
		"least-accessed",
		"largest-first":
		return true
	}
	return false
}

func (conf *cacheflushConfig) loadConfiguration(configFile string) {
	fmt.Println("Loading config...")

	// Confirm config file exists, check local dir if not provided
	if configFile != "" {
		_, err := os.Stat(configFile)
		if os.IsNotExist(err) {
			fmt.Printf("Config file: %v does not exist", configFile)
			panic("Provided configuration file does not exist or has bad permissions.")
		}

	} else {
		fmt.Println("No config file specified, checking local directory for cacheflush.yaml...")

		_, err := os.Stat("cacheflush.yaml")
		if os.IsNotExist(err) {
			fmt.Println("No config file provided and cacheflush.yaml not found in execution directory.")
			panic("No config file provided and cacheflush.yaml not found in execution directory.")
		} else {
			configFile = "cacheflush.yaml"
		}
	}

	// Load config file
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		fmt.Println("Failed to load config file: ", err)
		panic("Failed to load config file")
	}

	// Read config file into Config object
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		fmt.Println("Faled to Unmarshal config: ", err)
		panic("Faled to Unmarshal config")
	}

	// Validate Log Settings
	if conf.LogFile == "" {
		fmt.Println("LogFile not configured.")
		panic("LogFile not configured.")
	}

	// Validate Permissions Settings TODO

	// Validate Path Settings
	if conf.BackingPool == "" {
		fmt.Println("BackingPool not configured.")
		panic("BackingPool not configured.")
	} else {
		_, err := os.Stat(conf.BackingPool)
		if os.IsNotExist(err) {
			fmt.Printf("Backing pool: %v does not exist", conf.BackingPool)
			panic("Configured BackingPool does not exist")
		}
	}

	if conf.CacheDrives == nil {
		fmt.Println("CacheDrives not configured.")
		panic("CacheDrives not configured.")
	} else {
		for _, cacheDrive := range conf.CacheDrives {
			_, err := os.Stat(cacheDrive)
			if os.IsNotExist(err) {
				fmt.Printf("Cache drive: %v does not exist or is inaccessible", cacheDrive)
				panic("Configured CacheDrive does not exist or is inaccessible")
			}
		}
	}

	// Validate Behavior Settings
	if conf.ForceFreeSpace == "" {
		fmt.Println("ForceFreeSpace not configured.")
		panic("ForceFreeSpace not configured.")
	} else {
		// TODO validate input string
	}
	if conf.MinimumAge == "" {
		fmt.Println("MinimumAge not configured.")
		panic("MinimumAge not configured.")
	} else {
		// TODO validate input string
	}
	if conf.CurrentAccessThreshold == "" {
		fmt.Println("CurrentAccessThreshold not configured.")
		panic("CurrentAccessThreshold not configured.")
	} else {
		// TODO validate input string
	}
	if conf.FlushPolicy == "" {
		fmt.Println("FlushPolicy not configured.")
		panic("FlushPolicy not configured.")
	} else if conf.isValidPolicy() == false {
		fmt.Println("&v is not a valid FlushPolicy.")
		panic("Invalid FlushPolicy configured.")
	}

	// Validate Pushover keys present if enabled
	if conf.PushoverEnabled == true {
		if conf.PushoverAppKey == "" {
			fmt.Println("Pushover enabled but AppKey not provided, disabling pushover")
			config.PushoverEnabled = false
		} else if conf.PushoverUserKey == "" {
			fmt.Println("Pushover enabled but UserKey not provided, disabling pushover")
			config.PushoverEnabled = false
		}
	}

	fmt.Printf("config: %+v\n", conf)
}
