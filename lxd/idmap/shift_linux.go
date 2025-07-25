//go:build linux && cgo

package idmap

/*
#cgo LDFLAGS: -lacl
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif
#include <byteswap.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/capability.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "../../lxd/include/lxd_posix_acl_xattr.h"
#include "../../lxd/include/memory_utils.h"
#include "../../lxd/include/mount_utils.h"
#include "../../lxd/include/process_utils.h"
#include "../../lxd/include/syscall_numbers.h"
#include "../../lxd/include/syscall_wrappers.h"

// Needs to be included at the end
#include <sys/acl.h>
#include <sys/xattr.h>

#ifndef VFS_CAP_REVISION_1
#define VFS_CAP_REVISION_1 0x01000000
#endif

#ifndef VFS_CAP_REVISION_2
#define VFS_CAP_REVISION_2 0x02000000
#endif

#ifndef VFS_CAP_REVISION_3
#define VFS_CAP_REVISION_3 0x03000000
struct vfs_ns_cap_data {
	__le32 magic_etc;
	struct {
		__le32 permitted;
		__le32 inheritable;
	} data[VFS_CAP_U32];
	__le32 rootid;
};
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
#define cpu_to_le16(w16) le16_to_cpu(w16)
#define le16_to_cpu(w16) ((u_int16_t)((u_int16_t)(w16) >> 8) | (u_int16_t)((u_int16_t)(w16) << 8))
#define cpu_to_le32(w32) le32_to_cpu(w32)
#define le32_to_cpu(w32)                                                                       \
	((u_int32_t)((u_int32_t)(w32) >> 24) | (u_int32_t)(((u_int32_t)(w32) >> 8) & 0xFF00) | \
	 (u_int32_t)(((u_int32_t)(w32) << 8) & 0xFF0000) | (u_int32_t)((u_int32_t)(w32) << 24))
#elif __BYTE_ORDER == __LITTLE_ENDIAN
#define cpu_to_le16(w16) ((u_int16_t)(w16))
#define le16_to_cpu(w16) ((u_int16_t)(w16))
#define cpu_to_le32(w32) ((u_int32_t)(w32))
#define le32_to_cpu(w32) ((u_int32_t)(w32))
#else
#error Expected endianess macro to be set
#endif

static __le32 native_to_le32(int n)
{
	return cpu_to_le32(n);
}

static int le16_to_native(__le16 n)
{
	return le16_to_cpu(n);
}

static int le32_to_native(__le32 n)
{
	return le32_to_cpu(n);
}

static int set_vfs_ns_caps(char *path, void *caps, ssize_t len, uint32_t uid)
{
	// Works because vfs_ns_cap_data is a superset of vfs_cap_data (rootid
	// field added to the end)
	struct vfs_ns_cap_data ns_xattr;

	memset(&ns_xattr, 0, sizeof(ns_xattr));
	memcpy(&ns_xattr, caps, len);
	ns_xattr.magic_etc &= ~(VFS_CAP_REVISION_1 | VFS_CAP_REVISION_2);
	ns_xattr.magic_etc |= VFS_CAP_REVISION_3;
	ns_xattr.rootid = cpu_to_le32(uid);

	return setxattr(path, "security.capability", &ns_xattr, sizeof(ns_xattr), 0);
}

static uid_t get_vfs_ns_caps_uid(void *caps, ssize_t len, struct vfs_ns_cap_data *ns_xattr)
{
	// Works because vfs_ns_cap_data is a superset of vfs_cap_data (rootid
	// field added to the end)

	memset(ns_xattr, 0, sizeof(*ns_xattr));
	memcpy(ns_xattr, caps, len);
	if (ns_xattr->magic_etc & VFS_CAP_REVISION_3)
		return le32_to_cpu(ns_xattr->rootid);

	return (uid_t)-1;
}

static void update_vfs_ns_caps_uid(void *caps, ssize_t len, struct vfs_ns_cap_data *ns_xattr, uint32_t uid)
{
	if (ns_xattr->magic_etc & VFS_CAP_REVISION_3)
		ns_xattr->rootid = cpu_to_le32(uid);

	memcpy(caps, ns_xattr, len);
}

int spoof_fs_ns_caps(const char *path)
{
	#define __raise_cap_permitted(x, ns_cap_data)   ns_cap_data.data[(x)>>5].permitted   |= (1<<((x)&31))

	struct vfs_ns_cap_data ns_xattr;

	memset(&ns_xattr, 0, sizeof(ns_xattr));
        __raise_cap_permitted(CAP_NET_RAW, ns_xattr);
	ns_xattr.magic_etc |= VFS_CAP_REVISION_3 | VFS_CAP_FLAGS_EFFECTIVE;
	ns_xattr.rootid = cpu_to_le32(1000000);

	return setxattr(path, "security.capability", &ns_xattr, sizeof(ns_xattr), 0);
}

int shiftowner(char *basepath, char *path, int uid, int gid)
{
	__do_close int fd = -EBADF;
	int ret;
	char fdpath[PATH_MAX], realpath[PATH_MAX];
	struct stat sb;

	fd = open(path, O_PATH | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0) {
		perror("Failed open");
		return 1;
	}

	ret = sprintf(fdpath, "/proc/self/fd/%d", fd);
	if (ret < 0) {
		perror("Failed sprintf");
		return 1;
	}

	ret = readlink(fdpath, realpath, PATH_MAX);
	if (ret < 0) {
		perror("Failed readlink");
		return 1;
	}

	if (strlen(realpath) < strlen(basepath)) {
		printf("Invalid path, source (%s) is outside of basepath (%s)\n", realpath, basepath);
		return 1;
	}

	if (strncmp(realpath, basepath, strlen(basepath))) {
		printf("Invalid path, source (%s) is outside of basepath " "(%s).\n", realpath, basepath);
		return 1;
	}

	ret = fstat(fd, &sb);
	if (ret < 0) {
		perror("Failed fstat");
		return 1;
	}

	ret = fchownat(fd, "", uid, gid, AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW);
	if (ret < 0) {
		perror("Failed chown");
		return 1;
	}

	if (!S_ISLNK(sb.st_mode)) {
		ret = chmod(fdpath, sb.st_mode);
		if (ret < 0) {
			perror("Failed chmod");
			return 1;
		}
	}

	return 0;
}

// Supported ACL a_version fields
#ifndef POSIX_ACL_XATTR_VERSION
struct posix_acl_xattr_entry {
	__le16 e_tag;
	__le16 e_perm;
	__le32 e_id;
};

struct posix_acl_xattr_header {
	__le32 a_version;
}
#endif

#ifndef ACL_USER_OBJ
#define ACL_USER_OBJ 0x01
#endif

#ifndef ACL_USER
#define ACL_USER 0x02
#endif

#ifndef ACL_GROUP_OBJ
#define ACL_GROUP_OBJ 0x04
#endif

#ifndef ACL_GROUP
#define ACL_GROUP 0x08
#endif

#ifndef ACL_MASK
#define ACL_MASK 0x10
#endif

#ifndef ACL_OTHER
#define ACL_OTHER 0x20
#endif

static inline int posix_acl_xattr_count(size_t size)
{
	if (size < sizeof(struct posix_acl_xattr_header))
		return -EINVAL;
	size -= sizeof(struct posix_acl_xattr_header);
	if (size % sizeof(struct posix_acl_xattr_entry))
		return -EINVAL;
	return size / sizeof(struct posix_acl_xattr_entry);
}

static void *posix_entry_start(void *value)
{
	struct posix_acl_xattr_header *header = value;
	struct posix_acl_xattr_entry *entry = (void *)(header + 1);
	return (void *)entry;
}

static void *posix_entry_end(void *value, size_t count)
{
	struct posix_acl_xattr_entry *entry = value;
	return (void *)(entry + count);
}

static void *posix_entry_next(void *value)
{
	struct posix_acl_xattr_entry *entry = value;
	return (void *)(entry + 1);
}

#define __STACK_SIZE (8 * 1024 * 1024)
static pid_t do_clone(int (*fn)(void *), void *arg, int flags)
{
	void *stack;

	stack = malloc(__STACK_SIZE);
	if (!stack)
		return -ENOMEM;

#ifdef __ia64__
	return __clone2(fn, stack, __STACK_SIZE, flags | SIGCHLD, arg, NULL);
#else
	return clone(fn, stack + __STACK_SIZE, flags | SIGCHLD, arg, NULL);
#endif
}

static int get_userns_fd_cb(void *data)
{
	return kill(getpid(), SIGSTOP);
}

static int get_userns_fd(void)
{
	int userns_fd = -EBADF;
	int file_fd = -EBADF;
	pid_t pid;
	char path[256];

	// Create the namespace.
	pid = do_clone(get_userns_fd_cb, NULL, CLONE_NEWUSER);
	if (pid < 0)
		goto err;

	// Fetch a reference.
	snprintf(path, sizeof(path), "/proc/%d/ns/user", pid);
	userns_fd = open(path, O_RDONLY | O_CLOEXEC);
	if (userns_fd < 0)
		goto err_process;

	// Setup uid_map
	snprintf(path, sizeof(path), "/proc/%d/uid_map", pid);
	file_fd = openat(AT_FDCWD, path, O_WRONLY);
	if (file_fd < 0)
		goto err_process;

	if (write(file_fd, "0 0 1", 5) != 5)
		goto err_process;

	if (close(file_fd) < 0) {
		file_fd = -EBADF;
		goto err_process;
	}

	// Setup setgroups
	snprintf(path, sizeof(path), "/proc/%d/setgroups", pid);
	file_fd = openat(AT_FDCWD, path, O_WRONLY);
	if (file_fd < 0)
		goto err_process;

	if (write(file_fd, "deny", 4) != 4)
		goto err_process;

	if (close(file_fd) < 0) {
		file_fd = -EBADF;
		goto err_process;
	}

	// Setup gid_map
	snprintf(path, sizeof(path), "/proc/%d/gid_map", pid);
	file_fd = openat(AT_FDCWD, path, O_WRONLY);
	if (file_fd < 0)
		goto err_process;

	if (write(file_fd, "0 0 1", 5) != 5)
		goto err_process;

	if (close(file_fd) < 0) {
		file_fd = -EBADF;
		goto err_process;
	}

	// Kill the temporary process.
	kill(pid, SIGKILL);
	wait_for_pid(pid);

	return userns_fd;

err_process:
	kill(pid, SIGKILL);
	wait_for_pid(pid);

err:
	close(userns_fd);
	close(file_fd);
	return -1;
}

static int create_detached_idmapped_mount(const char *path, const char *fstype)
{
	__do_close int fs_fd = -EBADF, mnt_fd = -EBADF, fd_userns = -EBADF;
	struct lxc_mount_attr attr = {
	    .attr_set		= MOUNT_ATTR_IDMAP,
	    .propagation	= MS_SLAVE,

	};
	int ret;

	if (strcmp(fstype, "") && strcmp(fstype, "none")) {
		fs_fd = lxd_fsopen(fstype, FSOPEN_CLOEXEC);
		if (fs_fd < 0)
			return -errno;

		ret = lxd_fsconfig(fs_fd, FSCONFIG_SET_STRING, "source", path, 0);
		if (ret < 0)
			return -errno;

		ret = lxd_fsconfig(fs_fd, FSCONFIG_CMD_CREATE, NULL, NULL, 0);
		if (ret < 0)
			return -errno;

		mnt_fd = lxd_fsmount(fs_fd, FSMOUNT_CLOEXEC, 0);
	} else {
		mnt_fd = lxd_open_tree(-EBADF, path, OPEN_TREE_CLONE | OPEN_TREE_CLOEXEC);
	}

	if (mnt_fd < 0)
		return -errno;

	fd_userns = get_userns_fd();
	if (fd_userns < 0)
		return -EBADF;

	attr.userns_fd = fd_userns;

	ret = lxd_mount_setattr(mnt_fd, "", AT_EMPTY_PATH, &attr, sizeof(attr));
	if (ret < 0)
		return -errno;

	close(fd_userns);
	return 0;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/unix"

	_ "github.com/canonical/lxd/lxd/include" // Used by cgo
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/logger"
)

// ShiftOwner updates uid and gid for a file when entering/exiting a namespace.
func ShiftOwner(basepath string, path string, uid int, gid int) error {
	cbasepath := C.CString(basepath)
	defer C.free(unsafe.Pointer(cbasepath))

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	r := C.shiftowner(cbasepath, cpath, C.int(uid), C.int(gid))
	if r != 0 {
		return fmt.Errorf("Failed to change ownership of: %s", path)
	}

	return nil
}

// GetCaps extracts the list of capabilities effective on the file.
func GetCaps(path string) ([]byte, error) {
	xattrs, err := shared.GetAllXattr(path)
	if err != nil {
		return nil, err
	}

	valueStr, ok := xattrs["security.capability"]
	if !ok {
		return nil, nil
	}

	return []byte(valueStr), nil
}

// SetCaps applies the caps for a particular root uid.
func SetCaps(path string, caps []byte, uid int64) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	ccaps := C.CBytes(caps)
	defer C.free(unsafe.Pointer(ccaps))

	r := C.set_vfs_ns_caps(cpath, ccaps, C.ssize_t(len(caps)), C.uint32_t(uid))
	if r != 0 {
		return fmt.Errorf("Failed to apply capabilities to: %s", path)
	}

	return nil
}

// ShiftACL updates uid and gid for file ACLs when entering/exiting a namespace.
func ShiftACL(path string, shiftIDs func(uid int64, gid int64) (int64, int64)) error {
	err := shiftACLType(path, C.ACL_TYPE_ACCESS, shiftIDs)
	if err != nil {
		return err
	}

	err = shiftACLType(path, C.ACL_TYPE_DEFAULT, shiftIDs)
	if err != nil {
		return err
	}

	return nil
}

func shiftACLType(path string, aclType int, shiftIDs func(uid int64, gid int64) (int64, int64)) error {
	// Convert the path to something usable with cgo
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	// Read the current ACL set for the requested type
	acl := C.acl_get_file(cpath, C.uint(aclType))
	if acl == nil {
		return nil
	}

	defer C.acl_free(unsafe.Pointer(acl))

	// Iterate through all ACL entries
	update := false
	for entryID := C.ACL_FIRST_ENTRY; ; entryID = C.ACL_NEXT_ENTRY {
		var ent C.acl_entry_t
		var tag C.acl_tag_t

		// Get the ACL entry
		ret := C.acl_get_entry(acl, C.int(entryID), &ent)
		if ret == 0 {
			break
		} else if ret < 0 {
			return fmt.Errorf("Failed to get the ACL entry for %s", path)
		}

		// Get the ACL type
		ret = C.acl_get_tag_type(ent, &tag)
		if ret == -1 {
			return fmt.Errorf("Failed to get the ACL type for %s", path)
		}

		// We only care about user and group ACLs, copy anything else
		if tag != C.ACL_USER && tag != C.ACL_GROUP {
			continue
		}

		// Get the value
		idp := (*C.id_t)(C.acl_get_qualifier(ent))
		if idp == nil {
			return fmt.Errorf("Failed to get current ACL value for %s", path)
		}

		// Shift the value
		newID := int64(-1)
		if tag == C.ACL_USER {
			newID, _ = shiftIDs((int64)(*idp), -1)
		} else {
			_, newID = shiftIDs(-1, (int64)(*idp))
		}

		// Update the new entry with the shifted value
		ret = C.acl_set_qualifier(ent, unsafe.Pointer(&newID))
		if ret == -1 {
			return fmt.Errorf("Failed to set ACL qualifier on %s", path)
		}

		update = true
	}

	// Update the on-disk ACLs to match
	if update {
		ret, err := C.acl_set_file(cpath, C.uint(aclType), acl)
		if ret < 0 {
			return fmt.Errorf("%s - Failed to change ACLs on %s", err, path)
		}
	}

	return nil
}

// SupportsVFS3Fscaps checks if VFS3Fscaps are supported.
func SupportsVFS3Fscaps(prefix string) bool {
	tmpfile, err := os.CreateTemp(prefix, ".lxd_fcaps_v3_")
	if err != nil {
		return false
	}

	defer func() { _ = tmpfile.Close() }()
	defer func() { _ = os.Remove(tmpfile.Name()) }()

	err = os.Chmod(tmpfile.Name(), 0001)
	if err != nil {
		return false
	}

	cpath := C.CString(tmpfile.Name())
	defer C.free(unsafe.Pointer(cpath))

	r := C.spoof_fs_ns_caps(cpath)
	if r != 0 {
		return false
	}

	cmd := exec.Command(tmpfile.Name())
	err = cmd.Run()
	if err != nil {
		errno, isErrno := shared.GetErrno(err)
		if isErrno && (errno == unix.ERANGE || errno == unix.EOVERFLOW) {
			return false
		}

		return true
	}

	return true
}

// UnshiftACL performs an UID/GID unshift on the ACL xattr value in accordance with idmap (set) provided.
func UnshiftACL(value string, set *IdmapSet) (string, error) {
	if set == nil {
		return "", errors.New("Invalid IdmapSet supplied")
	}

	buf := []byte(value)
	cBuf := C.CBytes(buf)
	defer C.free(cBuf)
	var header *C.struct_posix_acl_xattr_header = (*C.struct_posix_acl_xattr_header)(cBuf)

	size := len(buf)
	if size < int(unsafe.Sizeof(*header)) {
		return "", errors.New("Invalid ACL size")
	}

	if header.a_version != C.native_to_le32(C.POSIX_ACL_XATTR_VERSION) {
		return "", fmt.Errorf("Invalid ACL header version %d != %d", header.a_version, C.native_to_le32(C.POSIX_ACL_XATTR_VERSION))
	}

	count := C.posix_acl_xattr_count(C.size_t(size))
	if count < 0 {
		return "", errors.New("Invalid ACL count")
	}

	if count == 0 {
		return "", errors.New("No valid ACLs found")
	}

	entryPtr := C.posix_entry_start(unsafe.Pointer(header))
	endEntryPtr := C.posix_entry_end(entryPtr, C.size_t(count))
	for entryPtr != endEntryPtr {
		entry := (*C.struct_posix_acl_xattr_entry)(entryPtr)
		switch C.le16_to_native(entry.e_tag) {
		case C.ACL_USER:
			ouid := int64(C.le32_to_native(entry.e_id))
			uid, _ := set.ShiftFromNs(ouid, -1)
			if int(uid) != -1 {
				entry.e_id = C.native_to_le32(C.int(uid))
				logger.Debugf("Unshifting ACL_USER from uid %d to uid %d", ouid, uid)
			}

		case C.ACL_GROUP:
			ogid := int64(C.le32_to_native(entry.e_id))
			_, gid := set.ShiftFromNs(-1, ogid)
			if int(gid) != -1 {
				entry.e_id = C.native_to_le32(C.int(gid))
				logger.Debugf("Unshifting ACL_GROUP from gid %d to gid %d", ogid, gid)
			}

		case C.ACL_USER_OBJ:
			logger.Debug("Ignoring ACL type ACL_USER_OBJ")
		case C.ACL_GROUP_OBJ:
			logger.Debug("Ignoring ACL type ACL_GROUP_OBJ")
		case C.ACL_MASK:
			logger.Debug("Ignoring ACL type ACL_MASK")
		case C.ACL_OTHER:
			logger.Debug("Ignoring ACL type ACL_OTHER")
		default:
			logger.Debugf("Ignoring unknown ACL type %d", C.le16_to_native(entry.e_tag))
		}

		entryPtr = C.posix_entry_next(entryPtr)
	}

	buf = C.GoBytes(cBuf, C.int(size))

	return string(buf), nil
}

// UnshiftCaps performs an UID/GID unshift on the security.capability xattr value in accordance with idmap (set) provided.
func UnshiftCaps(value string, set *IdmapSet) (string, error) {
	if set == nil {
		return "", errors.New("Invalid IdmapSet supplied")
	}

	buf := []byte(value)
	cBuf := C.CBytes(buf)
	defer C.free(cBuf)
	var nsXattr C.struct_vfs_ns_cap_data

	size := C.ssize_t(len(buf))
	ouid := C.get_vfs_ns_caps_uid(cBuf, size, &nsXattr)
	if ouid == C.LXC_INVALID_UID {
		return value, nil
	}

	uid, _ := set.ShiftFromNs(int64(ouid), -1)
	if int(uid) != -1 {
		C.update_vfs_ns_caps_uid(cBuf, size, &nsXattr, C.uid_t(uid))
		logger.Debugf("Unshifting vfs capabilities from uid %d to uid %d", ouid, uid)
	}

	buf = C.GoBytes(cBuf, C.int(size))
	return string(buf), nil
}

// IdmapStorageType represents a file system idmapping type.
type IdmapStorageType string

// Define IdmapStorageType type values.
const (
	IdmapStorageNone     = "none"
	IdmapStorageIdmapped = "idmapped"
)

// CanIdmapMount checks if (fstype) filesystem supports idmapped mounts.
func CanIdmapMount(path string, fstype string) bool {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cfstype := C.CString(fstype)
	defer C.free(unsafe.Pointer(cfstype))

	return bool(C.create_detached_idmapped_mount(cpath, cfstype) == 0)
}
