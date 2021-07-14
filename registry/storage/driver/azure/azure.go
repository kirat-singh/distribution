// Package azure provides a storagedriver.StorageDriver implementation to
// store blobs in Microsoft Azure Blob Storage Service.
package azure

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"

	azure "github.com/Azure/azure-sdk-for-go/storage"
)

const driverName = "azure"

const (
	paramAccountName      = "accountname"
	paramAccountKey       = "accountkey"
	paramConnectionString = "connectionstring"
	paramContainer        = "container"
	paramRealm            = "realm"
	paramRootDirectory    = "rootdirectory"
	maxChunkSize          = 4 * 1024 * 1024
)

type blobClient interface {
	GetBlobReference(path string) *azure.Blob
	GetContainerReference() *azure.Container
	GetCopySourceURL(blob *azure.Blob) (url string)
	GetURLFor(blob *azure.Blob, expires time.Time) (url string, err error)
	CreateContainer() (created bool, err error)
}

type simpleBlobClient struct {
	client    *azure.BlobStorageClient
	container string
}

func (client *simpleBlobClient) GetBlobReference(path string) *azure.Blob {
	return client.GetContainerReference().GetBlobReference(path)
}

func (client *simpleBlobClient) GetContainerReference() *azure.Container {
	return client.client.GetContainerReference(client.container)
}

func (client *simpleBlobClient) CreateContainer() (created bool, err error) {
	return client.GetContainerReference().CreateIfNotExists(nil)
}

func (client *simpleBlobClient) GetURLFor(blob *azure.Blob, expires time.Time) (url string, err error) {
	return blob.GetSASURI(azure.BlobSASOptions{
		BlobServiceSASPermissions: azure.BlobServiceSASPermissions{
			Read: true,
		},
		SASOptions: azure.SASOptions{
			Expiry: expires,
		},
	})
}

func (client *simpleBlobClient) GetCopySourceURL(blob *azure.Blob) (url string) {
	return blob.GetURL()
}

type accountSASBlobClient struct {
	client          *azure.BlobStorageClient
	accountSASToken string
	container       string
}

func NewAccountSASClient(connectionString, container string) (client *accountSASBlobClient, err error) {
	// build a map of connection string key/value pairs
	parts := map[string]string{}
	for _, pair := range strings.Split(connectionString, ";") {
		if pair == "" {
			continue
		}

		equalDex := strings.IndexByte(pair, '=')
		if equalDex <= 0 {
			return nil, fmt.Errorf("Invalid connection segment %q", pair)
		}

		value := strings.TrimSpace(pair[equalDex+1:])
		key := strings.TrimSpace(strings.ToLower(pair[:equalDex]))
		parts[key] = value
	}
	accountSASToken := parts["sharedaccesssignature"]
	if accountSASToken != "" {
		azClient, err := azure.NewClientFromConnectionString(connectionString)
		if err != nil {
			return nil, err
		}
		api := azClient.GetBlobService()
		sasClient := &accountSASBlobClient{
			client:          &api,
			accountSASToken: accountSASToken,
			container:       container,
		}
		return sasClient, nil
	}
	return nil, errors.New("Expected a SharedAccessSignature in the connection string")
}

func (client *accountSASBlobClient) GetBlobReference(path string) *azure.Blob {
	return client.GetContainerReference().GetBlobReference(path)
}

func (client *accountSASBlobClient) CreateContainer() (created bool, err error) {
	return false, nil
}

func (client *accountSASBlobClient) GetContainerReference() *azure.Container {
	return client.client.GetContainerReference(client.container)
}

func (client *accountSASBlobClient) GetURLFor(blob *azure.Blob, expires time.Time) (url string, err error) {
	// we just append the accountSAS token and ignore the expiry
	blobUrl := blob.GetURL()
	return blobUrl + "?" + client.accountSASToken, nil
}

func (client *accountSASBlobClient) GetCopySourceURL(blob *azure.Blob) (url string) {
	blobUrl := blob.GetURL()
	return blobUrl + "?" + client.accountSASToken
}

type driver struct {
	client        blobClient
	container     string
	rootDirectory string
}

type baseEmbed struct{ base.Base }

// Driver is a storagedriver.StorageDriver implementation backed by
// Microsoft Azure Blob Storage Service.
type Driver struct{ baseEmbed }

func init() {
	factory.Register(driverName, &azureDriverFactory{})
}

type azureDriverFactory struct{}

func (factory *azureDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters constructs a new Driver with a given parameters map.
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	rootDirectory, ok := parameters[paramRootDirectory]
	if !ok {
		rootDirectory = ""
	}

	container, ok := parameters[paramContainer]
	if !ok || fmt.Sprint(container) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramContainer)
	}

	connectionString, ok := parameters[paramConnectionString]
	if ok && fmt.Sprint(connectionString) != "" {
		// Create a connection string based client
		client, err := NewAccountSASClient(fmt.Sprint(connectionString), fmt.Sprint(container))
		if err != nil {
			return nil, err
		}
		return NewFromClient(client, fmt.Sprint(container), fmt.Sprint(rootDirectory))
	} else {
		// else look for accountname, accountkey and realm
		accountName, ok := parameters[paramAccountName]
		if !ok || fmt.Sprint(accountName) == "" {
			return nil, fmt.Errorf("no %s parameter provided", paramAccountName)
		}

		accountKey, ok := parameters[paramAccountKey]
		if !ok || fmt.Sprint(accountKey) == "" {
			return nil, fmt.Errorf("no %s parameter provided", paramAccountKey)
		}

		realm, ok := parameters[paramRealm]
		if !ok || fmt.Sprint(realm) == "" {
			realm = azure.DefaultBaseURL
		}

		return New(fmt.Sprint(accountName), fmt.Sprint(accountKey), fmt.Sprint(container), fmt.Sprint(realm), fmt.Sprint(rootDirectory))
	}
}

// New constructs a new Driver with the given Azure Storage Account credentials
func NewFromClient(client blobClient, container, rootDirectory string) (*Driver, error) {
	if _, err := client.CreateContainer(); err != nil {
		return nil, err
	}

	d := &driver{
		client:        client,
		container:     container,
		rootDirectory: rootDirectory}
	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

// New constructs a new Driver with the given Azure Storage Account credentials
func New(accountName, accountKey, container, realm, rootDirectory string) (*Driver, error) {
	api, err := azure.NewClient(accountName, accountKey, realm, azure.DefaultAPIVersion, true)
	if err != nil {
		return nil, err
	}

	blobService := api.GetBlobService()
	client := &simpleBlobClient{
		client:    &blobService,
		container: container,
	}

	return NewFromClient(client, container, rootDirectory)
}

// Implement the storagedriver.StorageDriver interface.
func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	blob, err := blobRef.Get(nil)
	if err != nil {
		if is404(err) {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		return nil, err
	}

	defer blob.Close()
	return ioutil.ReadAll(blob)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// max size for block blobs uploaded via single "Put Blob" for version after "2016-05-31"
	// https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks
	const limit = 256 * 1024 * 1024
	if len(contents) > limit {
		return fmt.Errorf("uploading %d bytes with PutContent is not supported; limit: %d bytes", len(contents), limit)
	}

	// Historically, blobs uploaded via PutContent used to be of type AppendBlob
	// (https://github.com/docker/distribution/pull/1438). We can't replace
	// these blobs atomically via a single "Put Blob" operation without
	// deleting them first. Once we detect they are BlockBlob type, we can
	// overwrite them with an atomically "Put Blob" operation.
	//
	// While we delete the blob and create a new one, there will be a small
	// window of inconsistency and if the Put Blob fails, we may end up with
	// losing the existing data while migrating it to BlockBlob type. However,
	// expectation is the clients pushing will be retrying when they get an error
	// response.
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	err := blobRef.GetProperties(nil)
	if err != nil && !is404(err) {
		return fmt.Errorf("failed to get blob properties: %v", err)
	}
	if err == nil && blobRef.Properties.BlobType != azure.BlobTypeBlock {
		if err := blobRef.Delete(nil); err != nil {
			return fmt.Errorf("failed to delete legacy blob (%s): %v", blobRef.Properties.BlobType, err)
		}
	}

	r := bytes.NewReader(contents)
	// reset properties to empty before doing overwrite
	blobRef.Properties = azure.BlobProperties{}
	return blobRef.CreateBlockBlobFromReader(r, nil)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	if ok, err := blobRef.Exists(); err != nil {
		return nil, err
	} else if !ok {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	err := blobRef.GetProperties(nil)
	if err != nil {
		return nil, err
	}
	info := blobRef.Properties
	size := info.ContentLength
	if offset >= size {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	resp, err := blobRef.GetRange(&azure.GetBlobRangeOptions{
		Range: &azure.BlobRange{
			Start: uint64(offset),
			End:   0,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	blobExists, err := blobRef.Exists()
	if err != nil {
		return nil, err
	}
	var size int64
	if blobExists {
		if append {
			err = blobRef.GetProperties(nil)
			if err != nil {
				return nil, err
			}
			blobProperties := blobRef.Properties
			size = blobProperties.ContentLength
		} else {
			err = blobRef.Delete(nil)
			if err != nil {
				return nil, err
			}
		}
	} else {
		if append {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		err = blobRef.PutAppendBlob(nil)
		if err != nil {
			return nil, err
		}
	}

	return d.newWriter(d.blobPath(path), size), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	blobPath := d.blobPath(path)
	blobRef := d.client.GetBlobReference(blobPath)
	// Check if the path is a blob
	if ok, err := blobRef.Exists(); err != nil {
		return nil, err
	} else if ok {
		err = blobRef.GetProperties(nil)
		if err != nil {
			return nil, err
		}
		blobProperties := blobRef.Properties

		return storagedriver.FileInfoInternal{FileInfoFields: storagedriver.FileInfoFields{
			Path:    path,
			Size:    blobProperties.ContentLength,
			ModTime: time.Time(blobProperties.LastModified),
			IsDir:   false,
		}}, nil
	}

	// Check if path is a virtual container
	virtContainerPath := blobPath
	if !strings.HasSuffix(virtContainerPath, "/") {
		virtContainerPath += "/"
	}

	containerRef := d.client.GetContainerReference()
	blobs, err := containerRef.ListBlobs(azure.ListBlobsParameters{
		Prefix:     virtContainerPath,
		MaxResults: 1,
	})
	if err != nil {
		return nil, err
	}
	if len(blobs.Blobs) > 0 {
		// path is a virtual container
		return storagedriver.FileInfoInternal{FileInfoFields: storagedriver.FileInfoFields{
			Path:  path,
			IsDir: true,
		}}, nil
	}

	// path is not a blob or virtual container
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	if path == "/" {
		path = ""
	}

	blobs, err := d.listBlobs(d.container, path)
	if err != nil {
		return blobs, err
	}

	list := directDescendants(blobs, path)
	if path != "" && len(list) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return list, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	srcBlobRef := d.client.GetBlobReference(d.blobPath(sourcePath))
	sourceBlobURL := d.client.GetCopySourceURL(srcBlobRef)
	destBlobRef := d.client.GetBlobReference(d.blobPath(destPath))
	err := destBlobRef.Copy(sourceBlobURL, nil)
	if err != nil {
		if is404(err) {
			return storagedriver.PathNotFoundError{Path: sourcePath}
		}
		return err
	}

	return srcBlobRef.Delete(nil)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	ok, err := blobRef.DeleteIfExists(nil)
	if err != nil {
		return err
	}
	if ok {
		return nil // was a blob and deleted, return
	}

	// Not a blob, see if path is a virtual container with blobs
	blobs, err := d.listBlobs(d.container, path)
	if err != nil {
		return err
	}

	for _, b := range blobs {
		blobRef = d.client.GetBlobReference(d.blobPath(b))
		if err = blobRef.Delete(nil); err != nil {
			return err
		}
	}

	if len(blobs) == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return nil
}

// URLFor returns a publicly accessible URL for the blob stored at given path
// for specified duration by making use of Azure Storage Shared Access Signatures (SAS).
// See https://msdn.microsoft.com/en-us/library/azure/ee395415.aspx for more info.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	expiresTime := time.Now().UTC().Add(20 * time.Minute) // default expiration
	expires, ok := options["expiry"]
	if ok {
		t, ok := expires.(time.Time)
		if ok {
			expiresTime = t
		}
	}
	blobRef := d.client.GetBlobReference(d.blobPath(path))
	return d.client.GetURLFor(blobRef, expiresTime)
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// directDescendants will find direct descendants (blobs or virtual containers)
// of from list of blob paths and will return their full paths. Elements in blobs
// list must be prefixed with a "/" and
//
// Example: direct descendants of "/" in {"/foo", "/bar/1", "/bar/2"} is
// {"/foo", "/bar"} and direct descendants of "bar" is {"/bar/1", "/bar/2"}
func directDescendants(blobs []string, prefix string) []string {
	if !strings.HasPrefix(prefix, "/") { // add trailing '/'
		prefix = "/" + prefix
	}
	if !strings.HasSuffix(prefix, "/") { // containerify the path
		prefix += "/"
	}

	out := make(map[string]bool)
	for _, b := range blobs {
		if strings.HasPrefix(b, prefix) {
			rel := b[len(prefix):]
			c := strings.Count(rel, "/")
			if c == 0 {
				out[b] = true
			} else {
				out[prefix+rel[:strings.Index(rel, "/")]] = true
			}
		}
	}

	var keys []string
	for k := range out {
		keys = append(keys, k)
	}
	return keys
}

func (d *driver) listBlobs(container, virtPath string) ([]string, error) {
	if virtPath != "" && !strings.HasSuffix(virtPath, "/") { // containerify the path
		virtPath += "/"
	}

	// we will replace the root directory prefix before returning blob names
	blobPrefix := d.blobPath("")

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if blobPrefix == "" {
		prefix = "/"
	}

	out := []string{}
	marker := ""
	containerRef := d.client.GetContainerReference()
	for {
		resp, err := containerRef.ListBlobs(azure.ListBlobsParameters{
			Marker: marker,
			Prefix: d.blobPath(virtPath),
		})

		if err != nil {
			return out, err
		}

		for _, b := range resp.Blobs {
			out = append(out, strings.Replace(b.Name, blobPrefix, prefix, 1))
		}

		if len(resp.Blobs) == 0 || resp.NextMarker == "" {
			break
		}
		marker = resp.NextMarker
	}
	return out, nil
}

func (d *driver) blobPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.rootDirectory, "/")+path, "/")
}

func is404(err error) bool {
	statusCodeErr, ok := err.(azure.AzureStorageServiceError)
	return ok && statusCodeErr.StatusCode == http.StatusNotFound
}

type writer struct {
	driver    *driver
	path      string
	size      int64
	bw        *bufio.Writer
	closed    bool
	committed bool
	cancelled bool
}

func (d *driver) newWriter(path string, size int64) storagedriver.FileWriter {
	return &writer{
		driver: d,
		path:   path,
		size:   size,
		bw: bufio.NewWriterSize(&blockWriter{
			client: d.client,
			path:   path,
		}, maxChunkSize),
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	n, err := w.bw.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}
	w.closed = true
	return w.bw.Flush()
}

func (w *writer) Cancel() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	blobRef := w.driver.client.GetBlobReference(w.path)
	return blobRef.Delete(nil)
}

func (w *writer) Commit() error {
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}
	w.committed = true
	return w.bw.Flush()
}

type blockWriter struct {
	client blobClient
	path   string
}

func (bw *blockWriter) Write(p []byte) (int, error) {
	n := 0
	blobRef := bw.client.GetBlobReference(bw.path)
	for offset := 0; offset < len(p); offset += maxChunkSize {
		chunkSize := maxChunkSize
		if offset+chunkSize > len(p) {
			chunkSize = len(p) - offset
		}
		err := blobRef.AppendBlock(p[offset:offset+chunkSize], nil)
		if err != nil {
			return n, err
		}

		n += chunkSize
	}

	return n, nil
}
