package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/blang/semver"
	"github.com/gregdel/pushover"
	"github.com/minio/minio/pkg/disk"
	"github.com/rhysd/go-github-selfupdate/selfupdate"
	"github.com/sirupsen/logrus"
)

const version = "0.0.1"

var config cacheflushConfig
var log = logrus.New()

type fileDetails struct {
	Name       string
	Path       string
	Size       int64
	ModTime    time.Time
	AccessTime time.Time
}

func main() {
	// Check for updates
	doSelfUpdate()

	// Parse input
	configFile := flag.String("config", "", "Path to configuration file.")
	flag.Parse()

	// Load configuration
	config.loadConfiguration(*configFile)

	// Setup logger
	file, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0664)
	if err != nil {
		fmt.Println("Failed to open/creater log file ", config.LogFile)
		panic("Failed to open/create log file")
	}
	defer file.Close()

	log.SetOutput(file)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	// Send Pushover notification that cacheflush starting
	log.Info("============= New CacheFlush Execution =============")
	sendPushoverMessage("Starting cacheflush...")

	// Loop over cache drives
	for _, cacheDrive := range config.CacheDrives {
		// Send notification
		log.Info("Processing cacheDrive: ", cacheDrive)
		sendPushoverMessage("Processing cacheDrive: ", cacheDrive)

		// Get disk free space
		diskInfo, _ := disk.GetInfo(cacheDrive)
		var startingFreeSpace int = int(diskInfo.Free)
		log.Info("Starting disk free space: ", startingFreeSpace)

		// Get cached files (flush method and settings processed in this function)
		okToMove, dontMove, tooNewToMove := getCachedFiles(cacheDrive)
		log.Debug("okToMove files: ", okToMove)
		log.Debug("dontMove files: ", okToMove)
		log.Debug("tooNewToMove files: ", okToMove)
		sendPushoverMessage(
			"okToMove files: ",
			string(len(okToMove)),
			"\ndontMove files: ",
			string(len(dontMove)),
			"\ntooNewToMove files: ",
			string(len(tooNewToMove)),
		)

		// Move the okToMove files
		for _, file := range okToMove {
			moveFile(file, cacheDrive)
		}

		var movedFiles int = len(okToMove)
		var requiredFreeSpace int = bytes(config.ForceFreeSpace)
		var diskFreeSpace int = startingFreeSpace

		// Flush dontMove files until drive meets ForceFreeSpace
		for _, file := range dontMove {
			diskInfo, _ := disk.GetInfo(cacheDrive)
			diskFreeSpace = int(diskInfo.Free)
			if diskFreeSpace <= requiredFreeSpace {
				break
			}
			moveFile(file, cacheDrive)
			movedFiles++
		}

		// Flush tooNewToMove files until drive meets ForceFressSpace
		for _, file := range tooNewToMove {
			diskInfo, _ := disk.GetInfo(cacheDrive)
			diskFreeSpace = int(diskInfo.Free)
			if diskFreeSpace <= requiredFreeSpace {
				break
			}
			moveFile(file, cacheDrive)
			movedFiles++
		}

		// Log disk completion
		log.Infof(
			"Done processing drive %v, moved %v files, free space before: %v GB, free space after: %v GB",
			cacheDrive,
			movedFiles,
			startingFreeSpace/1073741824,
			diskFreeSpace/1073741824,
		)
		sendPushoverMessage(
			"Done processing drive: ", cacheDrive,
			"\nMoved files: ", string(movedFiles),
			"\nFree Space Before: ", string(startingFreeSpace/1073741824),
			"GB\nFree Space After: ", string(diskFreeSpace/1073741824), "GB",
		)
	}

	// Send Pushover notification that snapsync is done
	log.Info("Cacheflush completed successfully")
	sendPushoverMessage("Cacheflush completed successfully")
}

func doSelfUpdate() {
	v := semver.MustParse(version)

	latest, err := selfupdate.UpdateSelf(v, "JeordyR/SnapSync")
	if err != nil {
		println("Binary update failed:", err)
		return
	}

	if latest.Version.Equals(v) {
		log.Println("Current binary is the latest version", latest.Version)
	} else {
		log.Println("Successfully updated to version", latest.Version)
		fmt.Println("Release note:\n", latest.ReleaseNotes)
	}
}

func sendPushoverMessage(message string, messageArgs ...string) {
	if config.PushoverEnabled == false {
		return
	}

	// Setup Pushover client and objects
	app := pushover.New(config.PushoverAppKey)
	recipient := pushover.NewRecipient(config.PushoverUserKey)

	// Setup message
	for _, v := range messageArgs {
		message = message + v
	}

	log.Info("Sending pushover notification, message: ", message)
	msg := pushover.NewMessage(message)

	// Send the message
	response, err := app.SendMessage(msg, recipient)
	if err != nil {
		log.Panic("Failed to send pushover message with error: ", err)
	}

	log.Debug("Pushover response: ", response)
}

func getCachedFiles(cacheDrive string) ([]fileDetails, []fileDetails, []fileDetails) {
	var dontMove []fileDetails
	var okToMove []fileDetails
	var tooNewToMove []fileDetails

	// Get all files on cacheDrive, ignoring any in OverrideDirectories
	err := filepath.Walk(cacheDrive, processFile(&dontMove, &okToMove, &tooNewToMove))
	if err != nil {
		log.Panic("Failed to parse files on cache drive with error: ", err)
	}

	// Sort dontMove and tooNewTomove slices based on FlushPolicy
	if config.FlushPolicy == "oldest-first" {
		// Oldest files (lowest epoch time) first
		sort.Slice(dontMove, func(i, j int) bool {
			return dontMove[i].ModTime.Unix() < dontMove[j].ModTime.Unix()
		})
		sort.Slice(tooNewToMove, func(i, j int) bool {
			return dontMove[i].ModTime.Unix() < dontMove[j].ModTime.Unix()
		})
	} else if config.FlushPolicy == "least-accessed" {
		// Least/oldest accessed files (lowest epoch time) first
		sort.Slice(dontMove, func(i, j int) bool {
			return dontMove[i].AccessTime.Unix() < dontMove[j].AccessTime.Unix()
		})
		sort.Slice(tooNewToMove, func(i, j int) bool {
			return tooNewToMove[i].AccessTime.Unix() < tooNewToMove[j].AccessTime.Unix()
		})
	} else if config.FlushPolicy == "largest-first" {
		// Largest files first
		sort.Slice(dontMove, func(i, j int) bool {
			return dontMove[i].Size > dontMove[j].Size
		})
		sort.Slice(tooNewToMove, func(i, j int) bool {
			return tooNewToMove[i].Size > tooNewToMove[j].Size
		})
	}

	return okToMove, dontMove, tooNewToMove
}

func processFile(dontMove *[]fileDetails, okToMove *[]fileDetails, tooNewToMove *[]fileDetails) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Error("Encoutered error walking files: ", err)
		}

		// Ignore directories
		if info.IsDir() {
			return nil
		}

		// Check if any overrides are present
		for _, override := range config.OverrideDirectories {
			if strings.Contains(path, override) {
				return nil
			}
		}

		// Format object
		var fileDetail fileDetails = fileDetails{
			Name:       info.Name(),
			Path:       path,
			Size:       info.Size(),
			ModTime:    info.ModTime(),
			AccessTime: info.Sys().(*syscall.Stat_t).Atim,
		}

		// Process files into slices based on config filters
		if config.MaximumAge != "" && time.Now().Unix()-fileDetail.ModTime.Unix() >= seconds(config.MaximumAge) {
			// File older than MaximumAge threshold
			*okToMove = append(*okToMove, fileDetail)
		} else if config.MinimumAge != "" && time.Now().Unix()-fileDetail.ModTime.Unix() <= seconds(config.MinimumAge) {
			// File newer than MinimumAge threshold
			*tooNewToMove = append(*tooNewToMove, fileDetail)
		} else if config.CurrentAccessThreshold != "" && time.Now().Unix()-fileDetail.AccessTime.Unix() <= seconds(config.CurrentAccessThreshold) {
			// File accessed more recently than current access threshold
			*dontMove = append(*dontMove, fileDetail)
		} else {
			*okToMove = append(*okToMove, fileDetail)
		}

		return nil
	}
}

func seconds(duration string) int64 {
	durationRegex := regexp.MustCompile(`(?P<years>\d+y)?(?P<months>\d+M)?(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?`)
	matches := durationRegex.FindStringSubmatch(duration)

	years := ParseInt64(matches[1])
	months := ParseInt64(matches[2])
	days := ParseInt64(matches[3])
	hours := ParseInt64(matches[4])
	minutes := ParseInt64(matches[5])
	seconds := ParseInt64(matches[6])

	hour := int64(time.Hour.Seconds())
	minute := int64(time.Minute.Seconds())
	second := int64(time.Second.Seconds())
	return years*24*365*hour + months*30*24*hour + days*24*hour + hours*hour + minutes*minute + seconds*second
}

func ParseInt64(value string) int64 {
	if len(value) == 0 {
		return 0
	}
	parsed, err := strconv.Atoi(value[:len(value)-1])
	if err != nil {
		return 0
	}
	return int64(parsed)
}

func bytes(value string) int {
	sizeRegex := regexp.MustCompile(`(?P<years>\d+TB)?(?P<months>\d+GB)?(?P<days>\d+MB)?`)
	matches := sizeRegex.FindStringSubmatch(value)

	tb := ParseInt(matches[1]) * 1099511627776
	gb := ParseInt(matches[2]) * 1073741824
	mb := ParseInt(matches[3]) * 1048576

	return tb + gb + mb
}

func ParseInt(value string) int {
	if len(value) == 0 {
		return 0
	}
	parsed, err := strconv.Atoi(value[:len(value)-2])
	if err != nil {
		return 0
	}
	return parsed
}

func moveFile(sourceFile fileDetails, cacheDrive string) {
	// Format the destination file path based on BackingPool
	destinationFile := strings.Replace(sourceFile.Path, cacheDrive, config.BackingPool, -1)
	destinationDirectory := filepath.Dir(destinationFile)

	log.Debugf("Moving file %v to %v...", sourceFile, destinationFile)

	// Confirm source file exists, create destination directory path if not present on BackingPool TODO

	// Move the file from cacheDrive to BackingPool TOOD
}
