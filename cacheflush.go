package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	skipMove := flag.Bool("skipmove", false, "True/false if we should skip moving files, useful for debugging.")
	force := flag.Bool("force", false, "Force flush all files (not including override) regardless of age, access time, etc.")
	flag.Parse()

	// Load configuration
	config.loadConfiguration(*configFile)

	if *skipMove == true {
		config.SkipMove = true
	}
	if *force == true {
		config.Force = true
	}

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
	if config.DebugLogging == true {
		log.SetLevel(logrus.DebugLevel)
	} else {
		log.SetLevel(logrus.InfoLevel)
	}

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
		log.Infof("Starting disk free space: %vGB", startingFreeSpace/1073741824)

		// Get cached files (flush method and settings processed in this function)
		okToMove, recentAccess, newlyAdded := getCachedFiles(cacheDrive)
		log.Info("okToMove files: ", len(okToMove))
		log.Info("newlyAdded files: ", len(newlyAdded))
		log.Info("recentAccess files: ", len(recentAccess))
		sendPushoverMessage(
			"okToMove files: ",
			strconv.Itoa(len(okToMove)),
			"\nnewlyAdded files: ",
			strconv.Itoa(len(newlyAdded)),
			"\nrecentAccess files: ",
			strconv.Itoa(len(recentAccess)),
		)

		// Move the okToMove files
		for _, file := range okToMove {
			moveFile(file, cacheDrive)
		}

		var movedFiles int = len(okToMove)
		var requiredFreeSpace int = bytes(config.ForceFreeSpace)
		var diskFreeSpace int = startingFreeSpace

		// Flush newlyAdded files until drive meets ForceFreeSpace
		for _, file := range newlyAdded {
			diskInfo, _ := disk.GetInfo(cacheDrive)
			diskFreeSpace = int(diskInfo.Free)
			if diskFreeSpace >= requiredFreeSpace {
				log.Debugf("Disk free space of %vGB is greater than ForceFreeSpace of %v, skipping further movement of newlyAdded files.", diskFreeSpace/1073741824, config.ForceFreeSpace)
				break
			}
			log.Debugf("Disk free space of %vGB is lower than ForceFreeSpace of %v, moving additional newlyAdded file.", diskFreeSpace/1073741824, config.ForceFreeSpace)
			moveFile(file, cacheDrive)
			movedFiles++
		}

		// Flush recentlyAccessed files until drive meets ForceFreeSpace
		for _, file := range recentAccess {
			diskInfo, _ := disk.GetInfo(cacheDrive)
			diskFreeSpace = int(diskInfo.Free)
			if diskFreeSpace >= requiredFreeSpace {
				log.Debugf("Disk free space of %vGB is >= to ForceFreeSpace of %v, skipping further movement of dontMove files.", diskFreeSpace/1073741824, config.ForceFreeSpace)
				break
			}
			log.Debugf("Disk free space of %vGB is lower than ForceFreeSpace of %v, moving additional recentlyAccessed file.", diskFreeSpace/1073741824, config.ForceFreeSpace)
			moveFile(file, cacheDrive)
			movedFiles++
		}

		// Clean up empty dirs if enabled
		if config.ClearEmptyDirs == true {
			// Create destination directory path if not present on BackingPool
			cmd := exec.Command("find", cacheDrive, "-empty", "-type", "d", "-delete")
			cmd.Run()
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
			"\nMoved files: ", strconv.Itoa(movedFiles),
			"\nFree Space Before: ", strconv.Itoa(startingFreeSpace/1073741824),
			"GB\nFree Space After: ", strconv.Itoa(diskFreeSpace/1073741824), "GB",
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

	// Short sleep to ensure rapid messages arrive in order
	time.Sleep(1)
}

func getCachedFiles(cacheDrive string) ([]fileDetails, []fileDetails, []fileDetails) {
	var recentAccess []fileDetails
	var okToMove []fileDetails
	var newlyAdded []fileDetails

	// Get all files on cacheDrive, ignoring any in OverrideDirectories
	err := filepath.Walk(cacheDrive, processFile(&recentAccess, &okToMove, &newlyAdded))
	if err != nil {
		log.Panic("Failed to parse files on cache drive with error: ", err)
	}

	// Return early if forced
	if config.Force == true {
		return okToMove, recentAccess, newlyAdded
	}

	// Sort dontMove and tooNewTomove slices based on FlushPolicy
	if config.FlushPolicy == "oldest-first" {
		// Oldest files (lowest epoch time) first
		sort.Slice(recentAccess, func(i, j int) bool {
			return recentAccess[i].ModTime.Unix() < recentAccess[j].ModTime.Unix()
		})
		sort.Slice(newlyAdded, func(i, j int) bool {
			return newlyAdded[i].ModTime.Unix() < newlyAdded[j].ModTime.Unix()
		})
	} else if config.FlushPolicy == "least-accessed" {
		// Least/oldest accessed files (lowest epoch time) first
		sort.Slice(recentAccess, func(i, j int) bool {
			return recentAccess[i].AccessTime.Unix() < recentAccess[j].AccessTime.Unix()
		})
		sort.Slice(newlyAdded, func(i, j int) bool {
			return newlyAdded[i].AccessTime.Unix() < newlyAdded[j].AccessTime.Unix()
		})
	} else if config.FlushPolicy == "largest-first" {
		// Largest files first
		sort.Slice(recentAccess, func(i, j int) bool {
			return recentAccess[i].Size > recentAccess[j].Size
		})
		sort.Slice(newlyAdded, func(i, j int) bool {
			return newlyAdded[i].Size > newlyAdded[j].Size
		})
	}

	return okToMove, recentAccess, newlyAdded
}

func processFile(recentAccess *[]fileDetails, okToMove *[]fileDetails, newlyAdded *[]fileDetails) filepath.WalkFunc {
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
			AccessTime: time.Unix(info.Sys().(*syscall.Stat_t).Atim.Sec, 0),
		}

		// Process files into slices based on config filters
		if config.Force == true {
			log.Debugf("Force enabled, adding file %v to okToMove", fileDetail.Path)
			*okToMove = append(*okToMove, fileDetail)
		} else if config.CurrentAccessThreshold != "" && time.Now().Unix()-fileDetail.AccessTime.Unix() <= seconds(config.CurrentAccessThreshold) {
			// File accessed more recently than current access threshold
			log.Debugf("File %v has been accessed recently.", fileDetail.Path)
			*recentAccess = append(*recentAccess, fileDetail)
		} else if config.MinimumAge != "" && time.Now().Unix()-fileDetail.ModTime.Unix() <= seconds(config.MinimumAge) {
			// File newer than MinimumAge threshold
			log.Debugf("File %v is too new to move.", fileDetail.Path)
			*newlyAdded = append(*newlyAdded, fileDetail)
		} else {
			*okToMove = append(*okToMove, fileDetail)
		}

		return nil
	}
}

func seconds(duration string) int64 {
	durationRegex := regexp.MustCompile(`(?P<years>\d+y)?(?P<months>\d+M)?(?P<weeks>\d+w)?(?P<days>\d+d)?(?P<hours>\d+h)?(?P<minutes>\d+m)?(?P<seconds>\d+s)?`)
	matches := durationRegex.FindStringSubmatch(duration)

	years := parseInt64(matches[1])
	months := parseInt64(matches[2])
	weeks := parseInt64(matches[3])
	days := parseInt64(matches[4])
	hours := parseInt64(matches[5])
	minutes := parseInt64(matches[6])
	seconds := parseInt64(matches[7])

	hour := int64(time.Hour.Seconds())
	minute := int64(time.Minute.Seconds())
	second := int64(time.Second.Seconds())
	return years*365*24*hour + months*30*24*hour + weeks*7*24*hour + days*24*hour + hours*hour + minutes*minute + seconds*second
}

func parseInt64(value string) int64 {
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

	tb := parseInt(matches[1]) * 1099511627776
	gb := parseInt(matches[2]) * 1073741824
	mb := parseInt(matches[3]) * 1048576

	return tb + gb + mb
}

func parseInt(value string) int {
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

	if config.SkipMove == true {
		log.Debugf("Skipping move operation, would have moved: %v to %v", sourceFile.Path, destinationFile)
	} else {
		if _, err := os.Stat(destinationDirectory); os.IsNotExist(err) {
			// Create destination directory path if not present on BackingPool
			cmd := exec.Command("mkdir", "-p", destinationDirectory)
			_ = syscall.Umask(002)
			cmd.SysProcAttr = &syscall.SysProcAttr{}
			cmd.SysProcAttr.Credential = &syscall.Credential{Uid: config.OwnerUID, Gid: config.OwnerGID}
			cmd.Run()
		}

		// Move the file from cacheDrive to BackingPool
		if _, err := os.Stat(destinationFile); os.IsNotExist(err) {
			log.Debugf("Moving file %v to %v...", sourceFile.Path, destinationFile)

			err := moveOperation(sourceFile.Path, destinationFile)
			if err != nil {
				log.Error("Failed to move file with error: ", err)
			}

		} else {
			log.Errorf("Failed to move %v to %v, file already exists.", sourceFile.Path, destinationFile)
		}

	}
}

func moveOperation(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("Couldn't open source file: %s", err)
	}

	out, err := os.Create(dst)
	if err != nil {
		in.Close()
		return fmt.Errorf("Couldn't open dest file: %s", err)
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	in.Close()
	if err != nil {
		return fmt.Errorf("Writing to output file failed: %s", err)
	}

	err = out.Sync()
	if err != nil {
		return fmt.Errorf("Sync error: %s", err)
	}

	_, err = os.Stat(src)
	if err != nil {
		return fmt.Errorf("Stat error: %s", err)
	}

	err = os.Chown(dst, int(config.OwnerUID), int(config.OwnerGID))
	if err != nil {
		return fmt.Errorf("Chown error: %s", err)
	}

	err = os.Chmod(dst, 0775)
	if err != nil {
		return fmt.Errorf("Chmod error: %s", err)
	}

	err = os.Remove(src)
	if err != nil {
		return fmt.Errorf("Failed removing original file: %s", err)
	}
	return nil
}
