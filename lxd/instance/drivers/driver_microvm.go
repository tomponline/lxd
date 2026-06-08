package drivers

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kballard/go-shellquote"
	"golang.org/x/sys/unix"

	"github.com/canonical/lxd/lxd/apparmor"
	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/device"
	deviceConfig "github.com/canonical/lxd/lxd/device/config"
	"github.com/canonical/lxd/lxd/instance"
	"github.com/canonical/lxd/lxd/instance/drivers/qmp"
	"github.com/canonical/lxd/lxd/instance/instancetype"
	"github.com/canonical/lxd/lxd/instance/operationlock"
	"github.com/canonical/lxd/lxd/lifecycle"
	"github.com/canonical/lxd/lxd/state"
	storagePools "github.com/canonical/lxd/lxd/storage"
	storageDrivers "github.com/canonical/lxd/lxd/storage/drivers"
	"github.com/canonical/lxd/lxd/storage/filesystem"
	"github.com/canonical/lxd/lxd/subprocess"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/ioprogress"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/osarch"
	"github.com/canonical/lxd/shared/revert"
)

// microvmLoad creates a MicroVM instance from the supplied InstanceArgs.
func microvmLoad(s *state.State, args db.InstanceArgs, p api.Project) (instance.Instance, error) {
	// Create the instance struct.
	d := microvmInstantiate(s, args, nil, p)

	// Expand config and devices.
	err := d.expandConfig()
	if err != nil {
		return nil, err
	}

	return d, nil
}

// microvmInstantiate creates a MicroVM struct without expanding config.
func microvmInstantiate(s *state.State, args db.InstanceArgs, expandedDevices deviceConfig.Devices, p api.Project) *microvm {
	d := &microvm{
		qemu: qemu{
			common: common{
				state: s,

				architecture: args.Architecture,
				creationDate: args.CreationDate,
				dbType:       args.Type,
				description:  args.Description,
				ephemeral:    args.Ephemeral,
				expiryDate:   args.ExpiryDate,
				id:           args.ID,
				lastUsedDate: args.LastUsedDate,
				localConfig:  args.Config,
				localDevices: args.Devices,
				logger:       logger.AddContext(logger.Ctx{"instanceType": args.Type, "instance": args.Name, "project": args.Project}),
				name:         args.Name,
				node:         args.Node,
				profiles:     args.Profiles,
				project:      p,
				isSnapshot:   args.Snapshot,
				stateful:     args.Stateful,
			},
		},
	}

	// Get the architecture name.
	archName, err := osarch.ArchitectureName(d.architecture)
	if err == nil {
		d.architectureName = archName
	}

	// Cleanup the zero values.
	if d.expiryDate.IsZero() {
		d.expiryDate = time.Time{}
	}

	if d.creationDate.IsZero() {
		d.creationDate = time.Time{}
	}

	if d.lastUsedDate.IsZero() {
		d.lastUsedDate = time.Time{}
	}

	// This is passed during expanded config validation.
	if expandedDevices != nil {
		d.expandedDevices = expandedDevices
	}

	return d
}

// microvmCreate creates a new storage volume record and returns an initialised Instance.
// Returns a revert fail function that can be used to undo this function if a subsequent step fails.
func microvmCreate(ctx context.Context, s *state.State, args db.InstanceArgs, p api.Project) (instance.Instance, revert.Hook, error) {
	revert := revert.New()
	defer revert.Fail()

	// Create the instance struct.
	d := &microvm{
		qemu: qemu{
			common: common{
				state: s,

				architecture: args.Architecture,
				creationDate: args.CreationDate,
				dbType:       args.Type,
				description:  args.Description,
				ephemeral:    args.Ephemeral,
				expiryDate:   args.ExpiryDate,
				id:           args.ID,
				lastUsedDate: args.LastUsedDate,
				localConfig:  args.Config,
				localDevices: args.Devices,
				logger:       logger.AddContext(logger.Ctx{"instanceType": args.Type, "instance": args.Name, "project": args.Project}),
				name:         args.Name,
				node:         args.Node,
				profiles:     args.Profiles,
				project:      p,
				isSnapshot:   args.Snapshot,
				stateful:     args.Stateful,
			},
		},
	}

	// Get the architecture name.
	archName, err := osarch.ArchitectureName(d.architecture)
	if err == nil {
		d.architectureName = archName
	}

	// Cleanup the zero values.
	if d.expiryDate.IsZero() {
		d.expiryDate = time.Time{}
	}

	if d.creationDate.IsZero() {
		d.creationDate = time.Time{}
	}

	if d.lastUsedDate.IsZero() {
		d.lastUsedDate = time.Time{}
	}

	if args.Snapshot {
		d.logger.Info("Creating instance snapshot", logger.Ctx{"ephemeral": d.ephemeral})
	} else {
		d.logger.Info("Creating instance", logger.Ctx{"ephemeral": d.ephemeral})
	}

	// Load the config.
	err = d.init()
	if err != nil {
		return nil, nil, fmt.Errorf("Failed expanding config: %w", err)
	}

	// When not a snapshot, perform full validation.
	if !args.Snapshot {
		// Validate expanded config (allows mixed instance types for profiles).
		err = instance.ValidConfig(s.OS, d.expandedConfig, true, instancetype.Any)
		if err != nil {
			return nil, nil, fmt.Errorf("Invalid config: %w", err)
		}

		err = instance.ValidDevices(s, d.project, d.Type(), d.localDevices, d.expandedDevices)
		if err != nil {
			return nil, nil, fmt.Errorf("Invalid devices: %w", err)
		}
	}

	// Retrieve the instance's storage pool.
	_, rootDiskDevice, err := d.getRootDiskDevice()
	if err != nil {
		return nil, nil, fmt.Errorf("Failed getting root disk: %w", err)
	}

	if rootDiskDevice["pool"] == "" {
		return nil, nil, errors.New("The instance's root device is missing the pool property")
	}

	// Initialize the storage pool.
	d.storagePool, err = storagePools.LoadByName(d.state, rootDiskDevice["pool"])
	if err != nil {
		return nil, nil, fmt.Errorf("Failed loading storage pool: %w", err)
	}

	// Validate that the storage pool supports MicroVM.
	if d.storagePool.Driver().Info().Name != "dir" {
		return nil, nil, errors.New("MicroVM instances are only supported on dir storage pools")
	}

	volType, err := storagePools.InstanceTypeToVolumeType(d.Type())
	if err != nil {
		return nil, nil, err
	}

	storagePoolSupported := slices.Contains(d.storagePool.Driver().Info().VolumeTypes, volType)

	if !storagePoolSupported {
		return nil, nil, errors.New("Storage pool does not support instance type")
	}

	if !d.IsSnapshot() {
		// Add devices to instance.
		cleanup, err := d.devicesAdd(d, false)
		if err != nil {
			return nil, nil, err
		}

		revert.Add(cleanup)
	}

	if d.isSnapshot {
		d.logger.Info("Created instance snapshot", logger.Ctx{"ephemeral": d.ephemeral})
	} else {
		d.logger.Info("Created instance", logger.Ctx{"ephemeral": d.ephemeral})
	}

	if d.isSnapshot {
		d.state.Events.SendLifecycle(d.project.Name, lifecycle.InstanceSnapshotCreated.Event(ctx, d, nil))
	} else {
		d.state.Events.SendLifecycle(d.project.Name, lifecycle.InstanceCreated.Event(ctx, d, map[string]any{
			"type":         api.InstanceTypeMicroVM,
			"storage-pool": d.storagePool.Name(),
			"location":     d.Location(),
		}))
	}

	cleanup := revert.Clone().Fail
	revert.Success()
	return d, cleanup, err
}

// microvm is the MicroVM instance driver, using QEMU's microvm machine type.
type microvm struct {
	qemu
}

// Type returns the instance type.
func (d *microvm) Type() instancetype.Type {
	return instancetype.MicroVM
}

// getKernelPath returns the path to the kernel to use for booting.
func (d *microvm) getKernelPath() string {
	kernelPath := d.expandedConfig["microvm.kernel_path"]
	if kernelPath == "" {
		// Default to the host's current kernel.
		kernelPath = "/boot/vmlinuz"
	}

	// Resolve symlinks to get the actual kernel file.
	resolved, err := filepath.EvalSymlinks(kernelPath)
	if err == nil {
		kernelPath = resolved
	}

	return kernelPath
}

// getInitrdPath returns the path to the initrd to use for booting.
func (d *microvm) getInitrdPath() string {
	initrdPath := d.expandedConfig["microvm.initrd_path"]
	if initrdPath == "" {
		// Default to the host's current initrd.
		initrdPath = "/boot/initrd.img"
	}

	// Resolve symlinks to get the actual initrd file.
	resolved, err := filepath.EvalSymlinks(initrdPath)
	if err == nil {
		initrdPath = resolved
	}

	return initrdPath
}

// getKernelAppend returns additional kernel command line arguments.
func (d *microvm) getKernelAppend() string {
	return d.expandedConfig["microvm.kernel_append"]
}

// Start starts the MicroVM instance using QEMU's microvm machine type with direct kernel boot.
func (d *microvm) Start(ctx context.Context, stateful bool, progressReporter ioprogress.ProgressReporter) error {
	unlock, err := d.updateBackupFileLock(context.Background())
	if err != nil {
		return err
	}

	defer unlock()

	d.logger.Debug("Start started", logger.Ctx{"stateful": stateful})
	defer d.logger.Debug("Start finished", logger.Ctx{"stateful": stateful})

	// Check that we are startable before creating an operation lock.
	err = d.validateStartup(stateful, d.statusCode())
	if err != nil {
		return err
	}

	// MicroVM only supports x86_64.
	if d.architecture != osarch.ARCH_64BIT_INTEL_X86 {
		return errors.New("MicroVM is only supported on x86_64 architecture")
	}

	// MicroVM does not support stateful snapshots.
	if stateful {
		return errors.New("MicroVM does not support stateful snapshots")
	}

	// Validate kernel and initrd paths.
	kernelPath := d.getKernelPath()
	if !shared.PathExists(kernelPath) {
		return fmt.Errorf("Kernel not found at %q", kernelPath)
	}

	initrdPath := d.getInitrdPath()
	if !shared.PathExists(initrdPath) {
		return fmt.Errorf("Initrd not found at %q", initrdPath)
	}

	// Setup a new operation.
	op, err := operationlock.CreateWaitGet(d.Project().Name, d.Name(), operationlock.ActionStart, []operationlock.Action{operationlock.ActionRestart, operationlock.ActionRestore}, false, false)
	if err != nil {
		if errors.Is(err, operationlock.ErrNonReusableSucceeded) {
			// An existing matching operation has now succeeded, return.
			return nil
		}

		return fmt.Errorf("Failed creating instance start operation: %w", err)
	}

	defer op.Done(err)

	// Ensure the correct vhost_vsock kernel module is loaded before establishing the vsock.
	err = util.LoadModule("vhost_vsock")
	if err != nil {
		op.Done(err)
		return err
	}

	revert := revert.New()
	defer revert.Fail()

	// Rotate the log file.
	logfile := d.LogFilePath()
	err = os.Rename(logfile, logfile+".old")
	if err != nil && !os.IsNotExist(err) {
		op.Done(err)
		return err
	}

	// Remove old pid file if needed.
	pidFilePath := d.pidFilePath()
	err = os.Remove(pidFilePath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		op.Done(err)
		return fmt.Errorf("Failed removing old PID file %q: %w", pidFilePath, err)
	}

	// Mount the instance's config volume.
	mountInfo, err := d.mount()
	if err != nil {
		op.Done(err)
		return err
	}

	revert.Add(func() { _ = d.unmount() })

	// Define a set of files to open and pass their file descriptors to QEMU command.
	fdFiles := make([]*os.File, 0)

	// Ensure passed files are closed after start has returned.
	defer func() {
		for _, file := range fdFiles {
			_ = file.Close()
		}
	}()

	// New or existing vsock ID from volatile.
	vsockID, vsockF, err := d.nextVsockID()
	if err != nil {
		return err
	}

	// Add allocated QEMU vhost file descriptor.
	vsockFD := d.addFileDescriptor(&fdFiles, vsockF)

	volatileSet := make(map[string]string)

	// Update vsock ID in volatile if needed for recovery.
	oldVsockID := d.localConfig["volatile.vsock_id"]
	newVsockID := strconv.FormatUint(uint64(vsockID), 10)
	if oldVsockID != newVsockID {
		volatileSet["volatile.vsock_id"] = newVsockID
	}

	// Generate UUID if not present.
	instUUID := d.localConfig["volatile.uuid"]
	if instUUID == "" {
		instUUID = uuid.New().String()
		volatileSet["volatile.uuid"] = instUUID
	}

	// Generate the config drive.
	err = d.generateConfigShare()
	if err != nil {
		op.Done(err)
		return err
	}

	// Create all needed paths.
	err = os.MkdirAll(d.LogPath(), 0700)
	if err != nil {
		op.Done(err)
		return err
	}

	err = os.MkdirAll(d.DevicesPath(), 0711)
	if err != nil {
		op.Done(err)
		return err
	}

	err = os.MkdirAll(d.ShmountsPath(), 0711)
	if err != nil {
		op.Done(err)
		return err
	}

	// Apply any volatile changes that need to be made.
	err = d.VolatileSet(volatileSet)
	if err != nil {
		op.Done(err)
		return err
	}

	devConfs := make([]*deviceConfig.RunConfig, 0, len(d.expandedDevices))
	postStartHooks := []func() error{}

	sortedDevices := d.expandedDevices.Sorted()
	startDevices := make([]device.Device, 0, len(sortedDevices))

	// Load devices in sorted order, this ensures that device mounts are added in path order.
	for _, entry := range sortedDevices {
		dev, err := d.deviceLoad(d, entry.Name, entry.Config)
		if err != nil {
			if errors.Is(err, device.ErrUnsupportedDevType) {
				continue // Skip unsupported device.
			}

			err = fmt.Errorf("Failed start validation for device %q: %w", entry.Name, err)
			op.Done(err)
			return err
		}

		// Run pre-start of check all devices before starting any device.
		err = dev.PreStartCheck()
		if err != nil {
			op.Done(err)
			return fmt.Errorf("Failed pre-start check for device %q: %w", dev.Name(), err)
		}

		startDevices = append(startDevices, dev)
	}

	// Start devices in order.
	for i := range startDevices {
		dev := startDevices[i]

		// Start the device.
		runConf, err := d.deviceStart(dev, false)
		if err != nil {
			err = fmt.Errorf("Failed starting device %q: %w", dev.Name(), err)
			op.Done(err)
			return err
		}

		revert.Add(func() {
			err := d.deviceStop(dev, false, "")
			if err != nil {
				d.logger.Error("Failed cleaning up device", logger.Ctx{"device": dev.Name(), "err": err})
			}
		})

		if runConf == nil {
			continue
		}

		if runConf.Revert != nil {
			revert.Add(runConf.Revert)
		}

		// Add post-start hooks
		if len(runConf.PostHooks) > 0 {
			postStartHooks = append(postStartHooks, runConf.PostHooks...)
		}

		devConfs = append(devConfs, runConf)
	}

	// Setup the config drive readonly bind mount.
	configMntPath := d.configDriveMountPath()
	err = d.configDriveMountPathClear()
	if err != nil {
		err = fmt.Errorf("Failed cleaning config drive mount path %q: %w", configMntPath, err)
		op.Done(err)
		return err
	}

	err = os.Mkdir(configMntPath, 0700)
	if err != nil {
		err = fmt.Errorf("Failed creating device mount path %q for config drive: %w", configMntPath, err)
		op.Done(err)
		return err
	}

	revert.Add(func() { _ = d.configDriveMountPathClear() })

	// Mount the config drive device as readonly.
	configSrcPath := filepath.Join(d.Path(), "config")
	err = device.DiskMount(configSrcPath, configMntPath, false, "", []string{"ro"}, "none")
	if err != nil {
		err = fmt.Errorf("Failed mounting device mount path %q for config drive: %w", configMntPath, err)
		op.Done(err)
		return err
	}

	// Note: virtiofs (vhost-user-fs) is not currently supported for microvm because the vhost-user
	// protocol has compatibility issues with the virtio-mmio transport used by microvm. The lxd-agent
	// will use 9p for the config drive instead.

	// Get qemu path for this architecture.
	qemuPath, _, err := d.qemuArchConfig(d.architecture)
	if err != nil {
		op.Done(err)
		return err
	}

	// Get the root disk path.
	rootDiskPath := ""
	for _, runConf := range devConfs {
		for _, mount := range runConf.Mounts {
			if mount.TargetPath == "/" {
				devSource, isPath := mountInfo.DevSource.(deviceConfig.DevSourcePath)
				if isPath {
					rootDiskPath = devSource.Path
				}

				break
			}
		}
	}

	if rootDiskPath == "" {
		err = errors.New("No root disk found")
		op.Done(err)
		return err
	}

	// Collect NIC configurations and open TAP file handles.
	var nics []microVMNIC
	for _, runConf := range devConfs {
		if len(runConf.NetworkInterface) > 0 {
			var devName, nicName, hwaddr, mtu string
			for _, nicItem := range runConf.NetworkInterface {
				switch nicItem.Key {
				case "devName":
					devName = nicItem.Value
				case "link":
					nicName = nicItem.Value
				case "hwaddr":
					hwaddr = nicItem.Value
				case "mtu":
					mtu = nicItem.Value
				}
			}

			if nicName == "" || hwaddr == "" {
				continue
			}

			// Open TAP file handle using TUNSETIFF ioctl.
			tapFile, err := os.OpenFile("/dev/net/tun", os.O_RDWR, 0)
			if err != nil {
				err = fmt.Errorf("Failed opening /dev/net/tun for NIC %q: %w", devName, err)
				op.Done(err)
				return err
			}

			revert.Add(func() { _ = tapFile.Close() })

			ifr, err := unix.NewIfreq(nicName)
			if err != nil {
				err = fmt.Errorf("Failed creating ifreq for NIC %q: %w", nicName, err)
				op.Done(err)
				return err
			}

			// Set flags for TAP device - must match what the TAP interface was created with
			// and what QEMU is expecting.
			ifr.SetUint16(unix.IFF_TAP | unix.IFF_NO_PI | unix.IFF_ONE_QUEUE | unix.IFF_MULTI_QUEUE | unix.IFF_VNET_HDR)

			err = unix.IoctlIfreq(int(tapFile.Fd()), unix.TUNSETIFF, ifr)
			if err != nil {
				err = fmt.Errorf("Failed getting TAP file handle for NIC %q: %w", nicName, err)
				op.Done(err)
				return err
			}

			// Add to file descriptors list.
			tapFD := d.addFileDescriptor(&fdFiles, tapFile)

			nics = append(nics, microVMNIC{
				devName: devName,
				nicName: nicName,
				hwaddr:  hwaddr,
				mtu:     mtu,
				tapFD:   tapFD,
			})
		}
	}

	// Generate MicroVM QEMU config.
	confFile, err := d.generateMicroVMConfigFile(vsockFD, rootDiskPath, configMntPath, nics, &fdFiles)
	if err != nil {
		op.Done(err)
		return err
	}

	// Configure memory limit.
	memSize := d.expandedConfig["limits.memory"]
	if memSize == "" {
		memSize = QEMUDefaultMemSize
	}

	// Parse memory size to bytes and convert to MB for QEMU.
	memSizeBytes, err := parseMemoryStr(memSize)
	if err != nil {
		err = fmt.Errorf("limits.memory invalid: %w", err)
		op.Done(err)
		return err
	}

	memSizeMB := memSizeBytes / 1024 / 1024

	// Build QEMU command.
	qemuCmd := []string{
		"--",
		qemuPath,
		"-S",
		"-name", d.Name(),
		"-uuid", instUUID,
		"-daemonize",
		"-cpu", "host",
		"-nographic",
		"-serial", "chardev:console",
		"-nodefaults",
		"-no-user-config",
		"-sandbox", "on,obsolete=deny,elevateprivileges=allow,spawn=allow,resourcecontrol=deny",
		"-readconfig", confFile,
		"-pidfile", d.pidFilePath(),
		"-D", d.LogFilePath(),
		"-m", fmt.Sprintf("%dM", memSizeMB),
		"-kernel", kernelPath,
		"-initrd", initrdPath,
	}

	// Build kernel command line.
	kernelAppend := "console=ttyS0 root=/dev/vda rw nocrypt cryptopts=skip"
	if extraAppend := d.getKernelAppend(); extraAppend != "" {
		kernelAppend = kernelAppend + " " + extraAppend
	}

	qemuCmd = append(qemuCmd, "-append", kernelAppend)

	// Handle raw.qemu.
	if d.expandedConfig["raw.qemu"] != "" {
		fields, err := shellquote.Split(d.expandedConfig["raw.qemu"])
		if err != nil {
			op.Done(err)
			return err
		}

		qemuCmd = append(qemuCmd, fields...)
	}

	// Run the qemu command via forklimits so we can selectively increase ulimits.
	forkLimitsCmd := []string{
		"forklimits",
	}

	if !d.state.OS.RunningInUserNS {
		// Required for PCI passthrough.
		forkLimitsCmd = append(forkLimitsCmd, "limit=memlock:unlimited:unlimited")
	}

	for i := range fdFiles {
		// Pass through any file descriptors as 3+i (as first 3 file descriptors are taken as standard).
		forkLimitsCmd = append(forkLimitsCmd, fmt.Sprintf("fd=%d", 3+i))
	}

	// Setup background process.
	earlyLogFilePath := d.EarlyLogFilePath()
	p, err := subprocess.NewProcess(d.state.OS.ExecPath, append(forkLimitsCmd, qemuCmd...), earlyLogFilePath, earlyLogFilePath)
	if err != nil {
		op.Done(err)
		return err
	}

	// Load the AppArmor profile
	err = apparmor.InstanceLoad(d.state.OS, d)
	if err != nil {
		op.Done(err)
		return err
	}

	p.SetApparmor(apparmor.InstanceProfileName(d))

	// Update the backup.yaml file.
	err = d.UpdateBackupFile()
	if err != nil {
		err = fmt.Errorf("Failed updating backup file: %w", err)
		op.Done(err)
		return err
	}

	err = p.StartWithFiles(context.Background(), fdFiles)
	if err != nil {
		op.Done(err)
		return err
	}

	_, err = p.Wait(context.Background())
	if err != nil {
		stderr, _ := os.ReadFile(earlyLogFilePath)
		err = fmt.Errorf("Failed running: %s: %s: %w", strings.Join(p.Args, " "), string(stderr), err)
		op.Done(err)
		return err
	}

	pid, err := d.pid()
	if err != nil || pid <= 0 {
		d.logger.Error("Failed getting VM process ID", logger.Ctx{"err": err, "pid": pid})
		op.Done(err)
		return err
	}

	revert.Add(func() {
		_ = d.killQemuProcess(pid)
	})

	// Start QMP monitoring.
	monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
	if err != nil {
		op.Done(err)
		return err
	}

	// Don't allow the monitor to trigger a disconnection shutdown event until cleanly started.
	monitor.SetOnDisconnectEvent(false)

	revert.Add(func() {
		monitor.Disconnect()
	})

	// Continue the VM.
	err = monitor.Start()
	if err != nil {
		op.Done(err)
		return err
	}

	// Record last state.
	err = d.recordLastState()
	if err != nil {
		op.Done(err)
		return err
	}

	// Run any post-start hooks.
	err = d.runHooks(postStartHooks)
	if err != nil {
		op.Done(err)
		return err
	}

	// Enable disconnection events after successful start.
	monitor.SetOnDisconnectEvent(true)

	d.state.Events.SendLifecycle(d.project.Name, lifecycle.InstanceStarted.Event(ctx, d, nil))

	revert.Success()

	d.logger.Info("Started instance", logger.Ctx{"pid": pid})

	return nil
}

// microVMNIC represents a NIC configuration for MicroVM.
type microVMNIC struct {
	devName string
	nicName string
	hwaddr  string
	mtu     string
	tapFD   int
}

// generateMicroVMConfigFile generates a QEMU config file for microvm machine type.
func (d *microvm) generateMicroVMConfigFile(vsockFD int, rootDiskPath string, configDrivePath string, nics []microVMNIC, fdFiles *[]*os.File) (string, error) {
	cfg := make([]cfgSection, 0, 16+len(nics)*2)

	// Machine configuration for microvm.
	cfg = append(cfg, cfgSection{
		name:    "machine",
		comment: "MicroVM Machine",
		entries: []cfgEntry{
			{key: "type", value: "microvm"},
			{key: "accel", value: "kvm"},
			{key: "pit", value: "off"},
			{key: "pic", value: "off"},
			{key: "rtc", value: "on"},
		},
	})

	// QMP socket.
	cfg = append(cfg, qemuControlSocket(&qemuControlSocketOpts{d.monitorPath()})...)

	// Console output.
	cfg = append(cfg, qemuConsole(&qemuConsoleOpts{d.consolePath()})...)

	// Vsock device for virtio-mmio.
	cfg = append(cfg, cfgSection{
		name:    "device",
		comment: "Vsock",
		entries: []cfgEntry{
			{key: "driver", value: "vhost-vsock-device"},
			{key: "guest-cid", value: d.localConfig["volatile.vsock_id"]},
			{key: "vhostfd", value: strconv.Itoa(vsockFD)},
		},
	})

	// Virtio-serial for lxd-agent status ringbuffer.
	// Ring buffer used by the lxd agent to report (write) its status to. LXD server will read
	// its content via QMP using "ringbuf-read" command.
	cfg = append(cfg, cfgSection{
		name:    `chardev "` + qemuSerialChardevName + `"`,
		comment: "LXD serial identifier",
		entries: []cfgEntry{
			{key: "backend", value: "ringbuf"},
			{key: "size", value: "16B"},
		},
	})

	cfg = append(cfg, cfgSection{
		name:    "device",
		comment: "Virtual serial bus",
		entries: []cfgEntry{
			{key: "driver", value: "virtio-serial-device"},
		},
	})

	cfg = append(cfg, cfgSection{
		name:    `device "qemu_serial"`,
		comment: "LXD serial port",
		entries: []cfgEntry{
			{key: "driver", value: "virtserialport"},
			{key: "name", value: "com.canonical.lxd"},
			{key: "chardev", value: qemuSerialChardevName},
		},
	})

	// Legacy serial port for backward compatibility with older lxd-agent-loader packages.
	cfg = append(cfg, cfgSection{
		name:    `device "qemu_serial_legacy"`,
		comment: "LXD legacy serial port",
		entries: []cfgEntry{
			{key: "driver", value: "virtserialport"},
			{key: "name", value: "org.linuxcontainers.lxd"},
		},
	})

	// Config drive using 9p for sharing lxd-agent and certificates.
	cfg = append(cfg, cfgSection{
		name:    `fsdev "dev-qemu_config-drive-9p"`,
		comment: "Config drive (9p)",
		entries: []cfgEntry{
			{key: "fsdriver", value: "local"},
			{key: "security_model", value: "none"},
			{key: "readonly", value: "on"},
			{key: "path", value: configDrivePath},
		},
	})

	cfg = append(cfg, cfgSection{
		name:    `device "dev-qemu_config-drive-9p"`,
		comment: "Config drive device",
		entries: []cfgEntry{
			{key: "driver", value: "virtio-9p-device"},
			{key: "mount_tag", value: "config"},
			{key: "fsdev", value: "dev-qemu_config-drive-9p"},
		},
	})

	// Root disk using virtio-blk-device for virtio-mmio.
	cfg = append(cfg, cfgSection{
		name:    "drive",
		comment: "Root disk drive",
		entries: []cfgEntry{
			{key: "id", value: "root"},
			{key: "file", value: rootDiskPath},
			{key: "format", value: "raw"},
			{key: "if", value: "none"},
			{key: "cache", value: "none"},
			{key: "aio", value: "io_uring"},
			{key: "discard", value: "unmap"},
		},
	})

	cfg = append(cfg, cfgSection{
		name:    "device",
		comment: "Root disk device",
		entries: []cfgEntry{
			{key: "driver", value: "virtio-blk-device"},
			{key: "drive", value: "root"},
			{key: "serial", value: "lxd_root"},
		},
	})

	// Add NIC devices.
	for _, nic := range nics {
		escapedDevName := filesystem.PathNameEncode(nic.devName)

		// Netdev configuration for the TAP device.
		netdevEntries := []cfgEntry{
			{key: "id", value: qemuDeviceNamePrefix + escapedDevName},
			{key: "type", value: "tap"},
			{key: "fd", value: strconv.Itoa(nic.tapFD)},
		}

		cfg = append(cfg, cfgSection{
			name:    "netdev",
			comment: fmt.Sprintf("Network device %s", nic.devName),
			entries: netdevEntries,
		})

		// Device configuration using virtio-net-device for virtio-mmio.
		devEntries := []cfgEntry{
			{key: "driver", value: "virtio-net-device"},
			{key: "netdev", value: qemuDeviceNamePrefix + escapedDevName},
			{key: "mac", value: nic.hwaddr},
		}

		cfg = append(cfg, cfgSection{
			name:    "device",
			comment: fmt.Sprintf("NIC %s", nic.devName),
			entries: devEntries,
		})
	}

	// Write the config file to disk.
	sb := qemuStringifyCfg(cfg...)
	configPath := filepath.Join(d.LogPath(), "qemu.conf")
	return configPath, os.WriteFile(configPath, []byte(sb.String()), 0640)
}

// Migrate is not supported for MicroVM instances.
func (d *microvm) Migrate(args *instance.CriuMigrationArgs) error {
	return storageDrivers.ErrNotSupported
}

// MigrateSend is not supported for MicroVM instances.
func (d *microvm) MigrateSend(ctx context.Context, args instance.MigrateSendArgs, progressReporter ioprogress.ProgressReporter) error {
	return storageDrivers.ErrNotSupported
}

// MigrateReceive is not supported for MicroVM instances.
func (d *microvm) MigrateReceive(ctx context.Context, args instance.MigrateReceiveArgs, progressReporter ioprogress.ProgressReporter) error {
	return storageDrivers.ErrNotSupported
}

// Snapshot is not supported for MicroVM instances initially.
func (d *microvm) Snapshot(ctx context.Context, name string, expiry *time.Time, stateful bool, diskVolumesMode string, progressReporter ioprogress.ProgressReporter) error {
	return storageDrivers.ErrNotSupported
}

// Shutdown attempts to gracefully shutdown the instance, but microvm doesn't support ACPI,
// so this falls back to an immediate Stop().
func (d *microvm) Shutdown(ctx context.Context, timeout time.Duration) error {
	d.logger.Debug("Shutdown requested, using Stop (microvm has no ACPI support)")
	return d.Stop(ctx, false)
}

// Stop stops the MicroVM instance.
func (d *microvm) Stop(ctx context.Context, stateful bool) error {
	d.logger.Debug("Stop started", logger.Ctx{"stateful": stateful})
	defer d.logger.Debug("Stop finished", logger.Ctx{"stateful": stateful})

	// Must be run prior to creating the operation lock.
	statusCode := d.statusCode()
	if !d.isRunningStatusCode(statusCode) && statusCode != api.Error && statusCode != api.Frozen {
		return ErrInstanceIsStopped
	}

	// MicroVM doesn't support stateful stop.
	if stateful {
		return errors.New("Stateful stop is not supported for MicroVM instances")
	}

	// Setup a new operation.
	op, err := operationlock.CreateWaitGet(d.Project().Name, d.Name(), operationlock.ActionStop, []operationlock.Action{operationlock.ActionRestart, operationlock.ActionRestore}, false, true)
	if err != nil {
		if errors.Is(err, operationlock.ErrNonReusableSucceeded) {
			return nil
		}

		return err
	}

	// Connect to the monitor.
	monitor, err := qmp.Connect(d.monitorPath(), qemuSerialChardevName, d.getMonitorEventHandler())
	if err != nil {
		d.logger.Warn("Failed connecting to monitor, forcing stop", logger.Ctx{"err": err})

		// Force stop the QEMU process.
		err = d.forceStop()
		if err != nil {
			op.Done(err)
			return err
		}

		// Wait for QEMU process to exit and perform device cleanup.
		err = d.qemu.onStop(ctx, "stop")
		if err != nil {
			op.Done(err)
			return err
		}

		d.state.Events.SendLifecycle(d.project.Name, lifecycle.InstanceStopped.Event(ctx, d, nil))

		op.Done(nil)
		return nil
	}

	// Get the wait channel.
	chDisconnect, err := monitor.Wait()
	if err != nil {
		d.logger.Warn("Failed getting monitor disconnection channel, forcing stop", logger.Ctx{"err": err})
		err = d.forceStop()
		if err != nil {
			op.Done(err)
			return err
		}
	} else {
		// Request the VM stop immediately.
		err = monitor.Quit()
		if err != nil {
			d.logger.Warn("Failed sending monitor quit command, forcing stop", logger.Ctx{"err": err})
			err = d.forceStop()
			if err != nil {
				op.Done(err)
				return err
			}
		}

		// Wait for QEMU to exit.
		ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		select {
		case <-chDisconnect:
		case <-ctxTimeout.Done():
			d.logger.Warn("Timed out waiting for monitor to disconnect, forcing stop")

			err = d.forceStop()
			if err != nil {
				op.Done(err)
				return err
			}
		}
	}

	// Wait for operation lock to be Done. This is normally completed by onStop which picks up the same
	// operation lock and then marks it as Done after the instance stops and the devices have been cleaned up.
	// However if the operation has failed for another reason we will collect the error here.
	err = op.Wait(context.Background())
	status := d.statusCode()
	if status != api.Stopped {
		errPrefix := fmt.Errorf("Failed stopping instance, status is %q", status)

		if err != nil {
			return fmt.Errorf("%s: %w", errPrefix.Error(), err)
		}

		return errPrefix
	}

	// Now handle errors from stop sequence and return to caller if wasn't completed cleanly.
	if err != nil {
		return err
	}

	return nil
}
