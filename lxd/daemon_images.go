package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	temporalEnums "go.temporal.io/api/enums/v1"
	temporalClient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/canonical/lxd/client"
	"github.com/canonical/lxd/lxd/db"
	"github.com/canonical/lxd/lxd/db/cluster"
	"github.com/canonical/lxd/lxd/lifecycle"
	"github.com/canonical/lxd/lxd/locking"
	"github.com/canonical/lxd/lxd/operations"
	"github.com/canonical/lxd/lxd/request"
	"github.com/canonical/lxd/lxd/rsync"
	"github.com/canonical/lxd/lxd/state"
	lxdTemporal "github.com/canonical/lxd/lxd/temporal"
	"github.com/canonical/lxd/lxd/util"
	"github.com/canonical/lxd/shared"
	"github.com/canonical/lxd/shared/api"
	"github.com/canonical/lxd/shared/cancel"
	"github.com/canonical/lxd/shared/logger"
	"github.com/canonical/lxd/shared/version"
	"github.com/google/uuid"
)

// ImageDownloadArgs used with ImageDownload.
type ImageDownloadArgs struct {
	ProjectName       string
	Server            string
	Protocol          string
	Certificate       string
	Secret            string
	Alias             string
	Type              string
	SetCached         bool
	PreferCached      bool
	AutoUpdate        bool
	Public            bool
	StoragePool       string
	Budget            int64
	SourceProjectName string
	UserRequested     bool
}

// imageOperationLock acquires a lock for operating on an image and returns the unlock function.
func imageOperationLock(fingerprint string) (locking.UnlockFunc, error) {
	l := logger.AddContext(logger.Ctx{"fingerprint": fingerprint})
	l.Debug("Acquiring lock for image")
	defer l.Debug("Lock acquired for image")

	return locking.Lock(context.TODO(), "ImageOperation_"+fingerprint)
}

func ImageDownloadWorkflow(ctx workflow.Context, fingerprint string, args ImageDownloadArgs) (*api.Image, error) {
	currentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID

	logger.Info("tomp started workflow", logger.Ctx{"id": currentWorkflowID, "member": lxdTemporal.StateFunc().ServerName})
	resourceID := "image-" + fingerprint

	mutex := lxdTemporal.NewMutex(currentWorkflowID, "default")
	unlockFunc, err := mutex.Lock(ctx, resourceID, 10*time.Minute)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = unlockFunc()
	}()

	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	var res api.Image
	err = workflow.ExecuteLocalActivity(ctx, imageDownload, fingerprint, args).Get(ctx, &res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

func imageDownloadClient(s *state.State, protocol string, serverCertificate string, serverURL string, sourceProjectName string) (lxd.ImageServer, error) {
	// Attempt to resolve the alias
	if slices.Contains([]string{"lxd", "simplestreams"}, protocol) {
		clientArgs := &lxd.ConnectionArgs{
			TLSServerCert: serverCertificate,
			UserAgent:     version.UserAgent,
			Proxy:         s.Proxy,
			CachePath:     s.OS.CacheDir,
			CacheExpiry:   time.Hour,
		}

		if protocol == "lxd" {
			// Setup LXD client
			remote, err := lxd.ConnectPublicLXD(serverURL, clientArgs)
			if err != nil {
				return nil, fmt.Errorf("Failed to connect to LXD server %q: %w", serverURL, err)
			}

			server, ok := remote.(lxd.InstanceServer)
			if ok {
				remote = server.UseProject(sourceProjectName)
			}

			return remote, nil
		} else {
			// Setup simplestreams client
			remote, err := lxd.ConnectSimpleStreams(serverURL, clientArgs)
			if err != nil {
				return nil, fmt.Errorf("Failed to connect to simple streams server %q: %w", serverURL, err)
			}

			return remote, nil
		}
	}

	return nil, errors.New("Invalid image protocol")
}

func ImageDownload(ctx context.Context, s *state.State, op *operations.Operation, args ImageDownloadArgs) (*api.Image, error) {
	var err error

	// Default protocol is LXD.
	if args.Protocol == "" {
		args.Protocol = "lxd"
	}

	fingerprint := args.Alias

	remote, err := imageDownloadClient(s, args.Protocol, args.Certificate, args.Server, args.SourceProjectName)
	if err != nil {
		return nil, err
	}

	// Attempt to resolve the alias
	// For public images, handle aliases and initial metadata
	if args.Secret == "" {
		// Look for a matching alias
		entry, _, err := remote.GetImageAliasType(args.Type, args.Alias)
		if err == nil {
			fingerprint = entry.Target // tomp do we need to expand partial fingerprint if resolved alias?
		}

		// Expand partial fingerprints
		imgInfo, _, err := remote.GetImage(fingerprint)
		if err != nil {
			return nil, fmt.Errorf("Failed getting remote image info: %w", err)
		}

		fingerprint = imgInfo.Fingerprint
	}

	id := "image-download-" + uuid.New().String()
	logger.Info("tomp schedule workflow", logger.Ctx{"id": id, "member": s.ServerName})
	run, err := s.TemporalClient.ExecuteWorkflow(context.Background(), temporalClient.StartWorkflowOptions{
		ID:                       id,
		TaskQueue:                lxdTemporal.LXDTaskQueue + s.ServerName,
		WorkflowIDReusePolicy:    temporalEnums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: temporalEnums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowTaskTimeout:      time.Minute * 2,
	}, ImageDownloadWorkflow, fingerprint, args)

	if err != nil {
		return nil, fmt.Errorf("Workflow failed to start: %w", err)
	}

	var result api.Image
	logger.Info("tomp waiting for workflow result", logger.Ctx{"id": id, "member": s.ServerName, "err": err})
	err = run.Get(context.Background(), &result)
	logger.Info("tomp got workflow result", logger.Ctx{"id": id, "member": s.ServerName, "err": err, "res": result})

	if err != nil {
		return nil, fmt.Errorf("Failed to get workflow result: %w", err)
	}

	return &result, nil

}

// ImageDownload resolves the image fingerprint and if not in the database, downloads it.
func imageDownload(ctx context.Context, fp string, args ImageDownloadArgs) (*api.Image, error) {
	s := lxdTemporal.StateFunc()

	l := logger.AddContext(logger.Ctx{"image": args.Alias, "fingerprint": fp, "member": s.ServerName, "project": args.ProjectName, "pool": args.StoragePool, "source": args.Server})

	l.Info("Image download starting")
	defer l.Info("Image download done")

	// time.Sleep(time.Second * 10)

	var err error
	var remote lxd.ImageServer
	var info *api.Image

	// Default protocol is LXD. Copy so that local modifications aren't propagated to args.
	protocol := args.Protocol
	if protocol == "" {
		protocol = "lxd"
	}

	// Copy so that local modifications aren't propagated to args.
	alias := args.Alias

	remote, err = imageDownloadClient(s, args.Protocol, args.Certificate, args.Server, args.SourceProjectName)
	if err != nil {
		return nil, err
	}

	// Ensure we are the only ones operating on this image.
	unlock, err := imageOperationLock(fp)
	if err != nil {
		return nil, err
	}

	defer unlock()

	// If auto-update is on and we're being given the image by
	// alias, try to use a locally cached image matching the given
	// server/protocol/alias, regardless of whether it's stale or
	// not (we can assume that it will be not *too* stale since
	// auto-update is on).
	interval := s.GlobalConfig.ImagesAutoUpdateIntervalHours()

	if args.PreferCached && interval > 0 && alias != fp {
		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			for _, architecture := range s.OS.Architectures {
				cachedFingerprint, err := tx.GetCachedImageSourceFingerprint(ctx, args.Server, args.Protocol, alias, args.Type, architecture)
				if err == nil && cachedFingerprint != fp {
					fp = cachedFingerprint
					break
				}
			}

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	var imgInfo *api.Image

	err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		// Check if the image already exists in this project (partial hash match).
		_, imgInfo, err = tx.GetImage(ctx, fp, cluster.ImageFilter{Project: &args.ProjectName})

		return err
	})
	if err == nil {
		var nodeAddress string

		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			// Check if the image is available locally or it's on another node.
			nodeAddress, err = tx.LocateImage(ctx, imgInfo.Fingerprint)

			return err
		})
		if err != nil {
			return nil, fmt.Errorf("Failed locating image %q in the cluster: %w", imgInfo.Fingerprint, err)
		}

		if nodeAddress != "" {
			// The image is available from another node, let's try to import it.
			err = instanceImageTransfer(ctx, s, args.ProjectName, args.ProjectName, imgInfo.Fingerprint, nodeAddress)
			if err != nil {
				return nil, fmt.Errorf("Failed transferring image %q from %q: %w", imgInfo.Fingerprint, nodeAddress, err)
			}

			err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
				// As the image record already exists in the project, just add the node ID to the image.
				return tx.AddImageToLocalNode(ctx, args.ProjectName, imgInfo.Fingerprint)
			})
			if err != nil {
				return nil, fmt.Errorf("Failed adding transferred image %q to local cluster member: %w", imgInfo.Fingerprint, err)
			}
		}
	} else if api.StatusErrorCheck(err, http.StatusNotFound) {
		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			// Check if the image already exists in some other project.
			_, imgInfo, err = tx.GetImageFromAnyProject(ctx, fp)

			return err
		})
		if err == nil {
			var nodeAddress string
			otherProject := imgInfo.Project

			err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
				// Check if the image is available locally or it's on another node. Do this before creating
				// the missing DB record so we don't include ourself in the search results.
				nodeAddress, err = tx.LocateImage(ctx, imgInfo.Fingerprint)
				if err != nil {
					return fmt.Errorf("Locate image %q in the cluster: %w", imgInfo.Fingerprint, err)
				}

				// We need to insert the database entry for this project, including the node ID entry.
				err = tx.CreateImage(ctx, args.ProjectName, imgInfo.Fingerprint, imgInfo.Filename, imgInfo.Size, args.Public, imgInfo.AutoUpdate, imgInfo.Architecture, imgInfo.CreatedAt, imgInfo.ExpiresAt, imgInfo.Properties, imgInfo.Type, nil)
				if err != nil {
					return fmt.Errorf("Failed creating image record for project: %w", err)
				}

				// Mark the image as "cached" if downloading for an instance.
				if args.SetCached {
					err = tx.SetImageCachedAndLastUseDate(ctx, args.ProjectName, imgInfo.Fingerprint, time.Now().UTC())
					if err != nil {
						return fmt.Errorf("Failed setting cached flag and last use date: %w", err)
					}
				}

				var id int

				id, imgInfo, err = tx.GetImage(ctx, fp, cluster.ImageFilter{Project: &args.ProjectName})
				if err != nil {
					return err
				}

				return tx.CreateImageSource(ctx, id, args.Server, args.Protocol, args.Certificate, alias)
			})
			if err != nil {
				return nil, err
			}

			// Transfer image if needed (after database record has been created above).
			if nodeAddress != "" {
				// The image is available from another node, let's try to import it.
				err = instanceImageTransfer(ctx, s, args.ProjectName, otherProject, info.Fingerprint, nodeAddress)
				if err != nil {
					return nil, fmt.Errorf("Failed transferring image: %w", err)
				}
			} else {
				// The image is available locally, copy the image files from the source project if these use different storage.
				if s.LocalConfig.StorageImagesVolume(otherProject) != s.LocalConfig.StorageImagesVolume(args.ProjectName) {
					sourcePath := filepath.Join(s.ImagesStoragePath(otherProject), imgInfo.Fingerprint)
					destPath := s.ImagesStoragePath(args.ProjectName)

					_, err = rsync.CopyFile(sourcePath, destPath, "", false)
					if err != nil {
						return nil, fmt.Errorf("Failed to copy image files from other project: %w", err)
					}

					if shared.PathExists(sourcePath + ".rootfs") {
						_, err = rsync.CopyFile(sourcePath+".rootfs", destPath, "", false)
						if err != nil {
							return nil, fmt.Errorf("Failed to copy image files from other project: %w", err)
						}
					}
				}
			}
		}
	}

	if imgInfo != nil {
		info = imgInfo
		l = l.AddContext(logger.Ctx{"fingerprint": info.Fingerprint, "autoUpdate": info.AutoUpdate, "imgProject": info.Project})
		l.Debug("Image already exists in the DB")

		var poolID int64
		var poolIDs []int64
		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			// If the image already exists, is cached and that it is
			// requested to be downloaded from an explicit `image copy` operation, then disable its `cache` parameter
			// so that it won't be candidate for auto removal.
			if imgInfo.Cached && args.UserRequested {
				err = tx.UnsetImageCached(ctx, args.ProjectName, imgInfo.Fingerprint)
				if err != nil {
					return err
				}
			}

			if args.StoragePool != "" {
				// Get the ID of the target storage pool.
				poolID, err = tx.GetStoragePoolID(ctx, args.StoragePool)
				if err != nil {
					return err
				}

				// Check if the image is already in the pool.
				poolIDs, err = tx.GetPoolsWithImage(ctx, info.Fingerprint)

				return err
			}

			return nil
		})
		if err != nil {
			return nil, err
		}

		// If not requested in a particular pool, we're done.
		if args.StoragePool == "" {
			return info, nil
		}

		if slices.Contains(poolIDs, poolID) {
			l.Debug("Image already exists on storage pool")
			return info, nil
		}

		// Import the image in the pool.
		l.Debug("Image does not exist on storage pool")

		err = imageCreateInPool(s, info, args.StoragePool, args.ProjectName)
		if err != nil {
			l.Debug("Failed to create image on storage pool", logger.Ctx{"err": err})
			return nil, fmt.Errorf("Failed to create image %q on storage pool %q: %w", info.Fingerprint, args.StoragePool, err)
		}

		l.Debug("Created image on storage pool")
		return info, nil
	}

	// Begin downloading
	/*if op != nil {
		l = l.AddContext(logger.Ctx{"trigger": op.URL(), "operation": op.ID()})
	}*/

	l.Info("Downloading image")

	// Cleanup any leftover from a past attempt
	destDir := s.ImagesStoragePath(args.ProjectName)
	destName := filepath.Join(destDir, fp)

	failure := true
	cleanup := func() {
		if failure {
			_ = os.Remove(destName)
			_ = os.Remove(destName + ".rootfs")
		}
	}
	defer cleanup()

	// Setup a progress handler
	/*progress := func(progress ioprogress.ProgressData) {
		if op == nil {
			return
		}

		meta := op.Metadata()
		if meta == nil {
			meta = make(map[string]any)
		}

		if meta["download_progress"] != progress.Text {
			meta["download_progress"] = progress.Text
			_ = op.UpdateMetadata(meta)
		}
	}*/

	var canceler *cancel.HTTPRequestCanceller
	canceler = cancel.NewHTTPRequestCanceller()

	/*if op != nil {
		op.SetCanceler(canceler)
	}*/

	switch protocol {
	case "lxd", "simplestreams":
		// Create the target files
		dest, err := os.Create(destName)
		if err != nil {
			return nil, err
		}

		defer func() { _ = dest.Close() }()

		destRootfs, err := os.Create(destName + ".rootfs")
		if err != nil {
			return nil, err
		}

		defer func() { _ = destRootfs.Close() }()

		// Get the image information
		if info == nil {
			if args.Secret != "" {
				info, _, err = remote.GetPrivateImage(fp, args.Secret)
				if err != nil {
					return nil, err
				}

				// Expand the fingerprint now and mark alias string to match
				fp = info.Fingerprint
				alias = info.Fingerprint
			} else {
				info, _, err = remote.GetImage(fp)
				if err != nil {
					return nil, err
				}
			}
		}

		// Compatibility with older LXD servers
		if info.Type == "" {
			info.Type = "container"
		}

		if args.Budget > 0 && info.Size > args.Budget {
			return nil, fmt.Errorf("Remote image with size %d exceeds allowed bugdget of %d", info.Size, args.Budget)
		}

		// Download the image
		var resp *lxd.ImageFileResponse
		request := lxd.ImageFileRequest{
			MetaFile:   io.WriteSeeker(dest),
			RootfsFile: io.WriteSeeker(destRootfs),
			//ProgressHandler: progress,
			Canceler: canceler,
			DeltaSourceRetriever: func(fingerprint string, file string) string {
				path := filepath.Join(destDir, fingerprint+"."+file)
				if shared.PathExists(path) {
					return path
				}

				return ""
			},
		}

		if args.Secret != "" {
			resp, err = remote.GetPrivateImageFile(fp, args.Secret, request)
		} else {
			resp, err = remote.GetImageFile(fp, request)
		}

		if err != nil {
			return nil, err
		}

		// Truncate down to size
		if resp.RootfsSize > 0 {
			err = destRootfs.Truncate(resp.RootfsSize)
			if err != nil {
				return nil, err
			}
		}

		err = dest.Truncate(resp.MetaSize)
		if err != nil {
			return nil, err
		}

		// Deal with unified images
		if resp.RootfsSize == 0 {
			err := os.Remove(destName + ".rootfs")
			if err != nil {
				return nil, err
			}
		}

		err = dest.Close()
		if err != nil {
			return nil, err
		}

		err = destRootfs.Close()
		if err != nil {
			return nil, err
		}

	case "direct":
		// Setup HTTP client
		httpClient, err := util.HTTPClient(args.Certificate, s.Proxy)
		if err != nil {
			return nil, err
		}

		// Use relatively short response header timeout so as not to hold the image lock open too long.
		httpTransport, ok := httpClient.Transport.(*http.Transport)
		if !ok {
			return nil, errors.New("Invalid http client type")
		}

		httpTransport.ResponseHeaderTimeout = 30 * time.Second

		req, err := http.NewRequest(http.MethodGet, args.Server, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("User-Agent", version.UserAgent)

		// Make the request
		raw, doneCh, err := cancel.CancelableDownload(canceler, httpClient.Do, req)
		if err != nil {
			return nil, err
		}

		defer close(doneCh)

		if raw.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Unable to fetch %q: %s", args.Server, raw.Status)
		}

		// Progress handler
		/*body := &ioprogress.ProgressReader{
			ReadCloser: raw.Body,
			Tracker: &ioprogress.ProgressTracker{
				Length: raw.ContentLength,
				Handler: func(percent int64, speed int64) {
					progress(ioprogress.ProgressData{Text: fmt.Sprintf("%d%% (%s/s)", percent, units.GetByteSizeString(speed, 2))})
				},
			},
		}*/

		// Create the target files
		f, err := os.Create(destName)
		if err != nil {
			return nil, err
		}

		defer func() { _ = f.Close() }()

		// Hashing
		sha256 := sha256.New()

		// Download the image
		writer := shared.NewQuotaWriter(io.MultiWriter(f, sha256), args.Budget)
		size, err := io.Copy(writer, raw.Body)
		if err != nil {
			return nil, err
		}

		// Validate hash
		result := hex.EncodeToString(sha256.Sum(nil))
		if result != fp {
			return nil, fmt.Errorf("Hash mismatch for %q: %s != %s", args.Server, result, fp)
		}

		// Parse the image
		imageMeta, imageType, err := getImageMetadata(destName)
		if err != nil {
			return nil, err
		}

		info = &api.Image{}
		info.Fingerprint = fp
		info.Size = size
		info.Architecture = imageMeta.Architecture
		info.CreatedAt = time.Unix(imageMeta.CreationDate, 0)
		info.ExpiresAt = time.Unix(imageMeta.ExpiryDate, 0)
		info.Properties = imageMeta.Properties
		info.Type = imageType

		err = f.Close()
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("Unsupported protocol: %v", protocol)
	}

	// Override visiblity
	info.Public = args.Public

	// We want to enable auto-update only if we were passed an
	// alias name, so we can figure when the associated
	// fingerprint changes in the remote.
	if alias != fp {
		info.AutoUpdate = args.AutoUpdate
	}

	err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
		// Create the database entry
		return tx.CreateImage(ctx, args.ProjectName, info.Fingerprint, info.Filename, info.Size, info.Public, info.AutoUpdate, info.Architecture, info.CreatedAt, info.ExpiresAt, info.Properties, info.Type, nil)
	})
	if err != nil {
		return nil, fmt.Errorf("Failed creating image record: %w", err)
	}

	// Image is in the DB now, don't wipe on-disk files on failure
	failure = false

	// Check if the image path changed (private images)
	newDestName := filepath.Join(destDir, fp)
	if newDestName != destName {
		err = shared.FileMove(destName, newDestName)
		if err != nil {
			return nil, err
		}

		if shared.PathExists(destName + ".rootfs") {
			err = shared.FileMove(destName+".rootfs", newDestName+".rootfs")
			if err != nil {
				return nil, err
			}
		}
	}

	// Record the image source
	if alias != fp {
		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			id, _, err := tx.GetImage(ctx, fp, cluster.ImageFilter{Project: &args.ProjectName})
			if err != nil {
				return err
			}

			return tx.CreateImageSource(ctx, id, args.Server, protocol, args.Certificate, alias)
		})
		if err != nil {
			return nil, err
		}
	}

	// Import into the requested storage pool
	if args.StoragePool != "" {
		err = imageCreateInPool(s, info, args.StoragePool, args.ProjectName)
		if err != nil {
			return nil, err
		}
	}

	// Mark the image as "cached" if downloading for an instance
	if args.SetCached {
		err = s.DB.Cluster.Transaction(context.TODO(), func(ctx context.Context, tx *db.ClusterTx) error {
			return tx.SetImageCachedAndLastUseDate(ctx, args.ProjectName, fp, time.Now().UTC())
		})
		if err != nil {
			return nil, fmt.Errorf("Failed setting cached flag and last use date: %w", err)
		}
	}

	l.Info("Image downloaded")

	var lifecycleRequestor *api.EventLifecycleRequestor
	//if op != nil {
	//		lifecycleRequestor = op.EventLifecycleRequestor()
	//	} else {
	lifecycleRequestor = request.CreateRequestor(ctx)
	//	}

	s.Events.SendLifecycle(args.ProjectName, lifecycle.ImageCreated.Event(info.Fingerprint, args.ProjectName, lifecycleRequestor, logger.Ctx{"type": info.Type}))

	return info, nil
}
