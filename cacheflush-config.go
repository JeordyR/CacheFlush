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

	// Runtime Settings
	RunUID int `yaml:"RunUID"`
	RunGID int `yaml:"RunGID"`

	// Path Settings
	BackingPool         string   `yaml:"BackingPool"`
	CacheDrives         []string `yaml:"CacheDrives"`
	OverrideDirectories []string `yaml:"OverrideDirectories"`

	// Behavior Settings
	ForceFreeSpace         string `yaml:"ForceFreeSpace"`
	MinimumAge             string `yaml:"MinimumAge"`
	MaximumAge             string `yaml:"MaximumAge"`
	CurrentAccessThreshold string `yaml:"CurrentAccessThreshold"`
	FlushPolicy            string `yaml:"FlushPolicy"`

	// Pushover Settings
	PushoverEnabled bool   `yaml:"PushoverEnabled,omitempty"`
	PushoverAppKey  string `yaml:"PushoverAppKey,omitempty"`
	PushoverUserKey string `yaml:"PushoverUserKey,omitempty"`
}

func (self *cacheflushConfig) isValidPolicy() bool {
	switch self.FlushPolicy {
	case
		"oldest-first",
		"least-accessed",
		"largest-first":
		return true
	}
	return false
}

func (self *cacheflushConfig) loadConfiguration(configFile string) {
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
	err = yaml.Unmarshal(data, &self)
	if err != nil {
		fmt.Println("Faled to Unmarshal config: ", err)
		panic("Faled to Unmarshal config")
	}

	// Validate Log Settings
	if self.LogFile == "" {
		fmt.Println("LogFile not configured.")
		panic("LogFile not configured.")
	}

	// Validate Runtime Settings
	if self.RunUID == 0 || self.RunGID == 0 {
		fmt.Println("RunUID and/or RunGID not configured.")
		panic("RunUID and/or RunGID not configured.")
	}

	// Validate Path Settings
	if self.BackingPool == "" {
		fmt.Println("BackingPool not configured.")
		panic("BackingPool not configured.")
	} else {
		_, err := os.Stat(self.BackingPool)
		if os.IsNotExist(err) {
			fmt.Printf("Backing pool: %v does not exist", self.BackingPool)
			panic("Configured BackingPool does not exist")
		}
	}

	if self.CacheDrives == nil {
		fmt.Println("CacheDrives not configured.")
		panic("CacheDrives not configured.")
	} else {
		for _, cacheDrive := range self.CacheDrives {
			_, err := os.Stat(cacheDrive)
			if os.IsNotExist(err) {
				fmt.Printf("Cache drive: %v does not exist or is inaccessible", cacheDrive)
				panic("Configured CacheDrive does not exist or is inaccessible")
			}
		}
	}

	// Validate Behavior Settings
	if self.ForceFreeSpace == "" {
		fmt.Println("ForceFreeSpace not configured.")
		panic("ForceFreeSpace not configured.")
	} else {
		// TODO validate input string
	}
	if self.MinimumAge == "" {
		fmt.Println("MinimumAge not configured.")
		panic("MinimumAge not configured.")
	} else {
		// TODO validate input string
	}
	if self.MaximumAge == "" {
		fmt.Println("MaximumAge not configured.")
		panic("MaximumAge not configured.")
	} else {
		// TODO validate input string
	}
	if self.CurrentAccessThreshold == "" {
		fmt.Println("CurrentAccessThreshold not configured.")
		panic("CurrentAccessThreshold not configured.")
	} else {
		// TODO validate input string
	}
	if self.FlushPolicy == "" {
		fmt.Println("FlushPolicy not configured.")
		panic("FlushPolicy not configured.")
	} else if self.isValidPolicy() == false {
		fmt.Println("&v is not a valid FlushPolicy.")
		panic("Invalid FlushPolicy configured.")
	}

	// Validate Pushover keys present if enabled
	if self.PushoverEnabled == true {
		if self.PushoverAppKey == "" {
			fmt.Println("Pushover enabled but AppKey not provided, disabling pushover")
			config.PushoverEnabled = false
		} else if self.PushoverUserKey == "" {
			fmt.Println("Pushover enabled but UserKey not provided, disabling pushover")
			config.PushoverEnabled = false
		}
	}

	fmt.Printf("config: %+v\n", self)
}
