// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

/*
A simple http server interface to Swarm
*/

package http

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/golang-lru"
	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/metrics"
	"github.com/gauss-project/eswarm/swarm/api"
	"github.com/gauss-project/eswarm/swarm/log"
	"github.com/gauss-project/eswarm/swarm/storage"
	"github.com/gauss-project/eswarm/swarm/storage/feed"
	"github.com/gauss-project/eswarm/swarm/util"
	"github.com/rs/cors"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)
import _ "net/http/pprof"

var (
	postRawCount    = metrics.NewRegisteredCounter("api.http.post.raw.count", nil)
	postRawFail     = metrics.NewRegisteredCounter("api.http.post.raw.fail", nil)
	postFilesCount  = metrics.NewRegisteredCounter("api.http.post.files.count", nil)
	postFilesFail   = metrics.NewRegisteredCounter("api.http.post.files.fail", nil)
	deleteCount     = metrics.NewRegisteredCounter("api.http.delete.count", nil)
	deleteFail      = metrics.NewRegisteredCounter("api.http.delete.fail", nil)
	getCount        = metrics.NewRegisteredCounter("api.http.get.count", nil)
	getFail         = metrics.NewRegisteredCounter("api.http.get.fail", nil)
	getFileCount    = metrics.NewRegisteredCounter("api.http.get.file.count", nil)
	getFileNotFound = metrics.NewRegisteredCounter("api.http.get.file.notfound", nil)
	getFileFail     = metrics.NewRegisteredCounter("api.http.get.file.fail", nil)
	getListCount    = metrics.NewRegisteredCounter("api.http.get.list.count", nil)
	getListFail     = metrics.NewRegisteredCounter("api.http.get.list.fail", nil)
)

type methodHandler map[string]http.Handler

func (m methodHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	v, ok := m[r.Method]
	if ok {
		v.ServeHTTP(rw, r)
		return
	}
	rw.WriteHeader(http.StatusMethodNotAllowed)
}

func NewServer(api *api.API, corsString string) *Server {
	var allowedOrigins []string
	for _, domain := range strings.Split(corsString, ",") {
		allowedOrigins = append(allowedOrigins, strings.TrimSpace(domain))
	}
	c := cors.New(cors.Options{
		AllowedOrigins: allowedOrigins,
		AllowedMethods: []string{http.MethodPost, http.MethodGet, http.MethodDelete, http.MethodPatch, http.MethodPut},
		MaxAge:         600,
		AllowedHeaders: []string{"*"},
	})
	//db, _ := leveldb.OpenFile("./cachelist.db", nil)

	server := &Server{api: api,
		//	db:db,
		m3u8: M3U8Opt{cachedDuration: 5000, centralRetrived: true},
	}

	defaultMiddlewares := []Adapter{
		RecoverPanic,
		SetRequestID,
		SetRequestHost,
		InitLoggingResponseWriter,
		ParseURI,
		InstrumentOpenTracing,
	}

	mux := http.NewServeMux()
	mux.Handle("/bzz:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleBzzGet),
			defaultMiddlewares...,
		),
		"POST": Adapt(
			http.HandlerFunc(server.HandlePostFiles),
			defaultMiddlewares...,
		),
		"DELETE": Adapt(
			http.HandlerFunc(server.HandleDelete),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/bzz-raw:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGet),
			defaultMiddlewares...,
		),
		"POST": Adapt(
			http.HandlerFunc(server.HandlePostRaw),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/bzz-immutable:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleBzzGet),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/bzz-hash:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGet),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/bzz-list:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetList),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/bzz-feed:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetFeed),
			defaultMiddlewares...,
		),
		"POST": Adapt(
			http.HandlerFunc(server.HandlePostFeed),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/chunk:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetChunk),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/metrics:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetMetrics),
			defaultMiddlewares...,
		),
		"POST": Adapt(
			http.HandlerFunc(server.HandlePostMetrics),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/m3u8:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetM3u8),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/file:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetVideoFile),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/activate:/", methodHandler{
		"POST": Adapt(
			http.HandlerFunc(server.HandleGetM3u8),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/receipts:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetReceived),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/nodes:/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleGetNodes),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/duration:/", methodHandler{
		"POST": Adapt(
			http.HandlerFunc(server.HandleDuration),
			defaultMiddlewares...,
		),
		"GET": Adapt(
			http.HandlerFunc(server.HandleDuration),
			defaultMiddlewares...,
		),
	})
	mux.Handle("/", methodHandler{
		"GET": Adapt(
			http.HandlerFunc(server.HandleRootPaths),
			SetRequestID,
			InitLoggingResponseWriter,
		),
	})
	server.Handler = c.Handler(mux)
	server.entries, _ = lru.New(10000)
	server.httpClient = util.CreateHttpReader()
	/*go func() {
		http.ListenAndServe("0.0.0.0:18080", nil)
	}()*/

	return server
}

func (s *Server) ListenAndServe(addr string) error {
	s.listenAddr = addr
	server := &http.Server{Addr: addr, Handler: s}
	s.srv = server
	return server.ListenAndServe()
	//return http.ListenAndServe(addr, s)
}

func (s *Server)Close(){
	if s.srv != nil {
		s.srv.Close()
	}
}

type M3U8Opt struct {
	cachedDuration  int32
	centralRetrived bool
}

// browser API for registering bzz url scheme handlers:
// https://developer.mozilla.org/en/docs/Web-based_protocol_handlers
// electron (chromium) api for registering bzz url scheme handlers:
// https://github.com/atom/electron/blob/master/docs/api/protocol.md
type Server struct {
	http.Handler
	api        *api.API
	listenAddr string
	chunks     uint64
	entries    *lru.Cache
	//保存一个和中心服务端的连接，是否长连接另外考虑
	httpClient *util.HttpReader
	//	db *leveldb.DB
	m3u8           M3U8Opt
	cachedDuration int32 //当前缓冲的数据值，以ms为单位
	srv 		*http.Server

}

func (s *Server) CreateCdnReporter(bzzAccount string, reportURLs []string) {
	s.httpClient.SetCdnReporter(bzzAccount, reportURLs)
}
func (s *Server) HandleBzzGet(w http.ResponseWriter, r *http.Request) {
	log.Debug("handleBzzGet", "ruid", GetRUID(r.Context()), "uri", r.RequestURI)
	if r.Header.Get("Accept") == "application/x-tar" {
		uri := GetURI(r.Context())
		_, credentials, _ := r.BasicAuth()
		reader, err := s.api.GetDirectoryTar(r.Context(), s.api.Decryptor(r.Context(), credentials), uri)
		if err != nil {
			if isDecryptError(err) {
				w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", uri.Address().String()))
				respondError(w, r, err.Error(), http.StatusUnauthorized)
				return
			}
			respondError(w, r, fmt.Sprintf("Had an error building the tarball: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/x-tar")

		fileName := uri.Addr
		if found := path.Base(uri.Path); found != "" && found != "." && found != "/" {
			fileName = found
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s.tar\"", fileName))

		w.WriteHeader(http.StatusOK)
		io.Copy(w, reader)
		return
	}

	s.HandleGetFile(w, r)
}

func (s *Server) HandleRootPaths(w http.ResponseWriter, r *http.Request) {
	switch r.RequestURI {
	case "/":
		respondTemplate(w, r, "landing-page", "Swarm: Please request a valid ENS or swarm hash with the appropriate bzz scheme", 200)
		return
	case "/robots.txt":
		w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
		fmt.Fprintf(w, "User-agent: *\nDisallow: /")
	case "/favicon.ico":
		w.WriteHeader(http.StatusOK)
		w.Write(faviconBytes)
	default:
		respondError(w, r, "Not Found", http.StatusNotFound)
	}
}

// HandlePostRaw handles a POST request to a raw bzz-raw:/ URI, stores the request
// body in swarm and returns the resulting storage address as a text/plain response
func (s *Server) HandlePostRaw(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	log.Debug("handle.post.raw", "ruid", ruid)

	postRawCount.Inc(1)

	toEncrypt := false
	uri := GetURI(r.Context())
	if uri.Addr == "encrypt" {
		toEncrypt = true
	}

	if uri.Path != "" {
		postRawFail.Inc(1)
		respondError(w, r, "raw POST request cannot contain a path", http.StatusBadRequest)
		return
	}

	if uri.Addr != "" && uri.Addr != "encrypt" {
		postRawFail.Inc(1)
		respondError(w, r, "raw POST request addr can only be empty or \"encrypt\"", http.StatusBadRequest)
		return
	}

	if r.Header.Get("Content-Length") == "" {
		postRawFail.Inc(1)
		respondError(w, r, "missing Content-Length header in request", http.StatusBadRequest)
		return
	}

	addr, _, err := s.api.Store(r.Context(), r.Body, r.ContentLength, toEncrypt)
	if err != nil {
		postRawFail.Inc(1)
		respondError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug("stored content", "ruid", ruid, "key", addr)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, addr)
}

// HandlePostFiles handles a POST request to
// bzz:/<hash>/<path> which contains either a single file or multiple files
// (either a tar archive or multipart form), adds those files either to an
// existing manifest or to a new manifest under <path> and returns the
// resulting manifest hash as a text/plain response
func (s *Server) HandlePostFiles(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	log.Trace("handle.post.files", "ruid", ruid)
	postFilesCount.Inc(1)

	contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		postFilesFail.Inc(1)
		respondError(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	toEncrypt := false
	uri := GetURI(r.Context())
	if uri.Addr == "encrypt" {
		toEncrypt = true
	}

	var addr storage.Address
	if uri.Addr != "" && uri.Addr != "encrypt" {
		addr, err = s.api.Resolve(r.Context(), uri.Addr)
		if err != nil {
			postFilesFail.Inc(1)
			respondError(w, r, fmt.Sprintf("cannot resolve %s: %s", uri.Addr, err), http.StatusInternalServerError)
			return
		}
		log.Trace("encrypted resolved key", "ruid", ruid, "key", addr)
	} else {
		addr, err = s.api.NewManifest(r.Context(), toEncrypt)
		if err != nil {
			postFilesFail.Inc(1)
			respondError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Trace("new manifest", "ruid", ruid, "key", addr, "time", time.Now())
	}

	newAddr, err := s.api.UpdateManifest(r.Context(), addr, func(mw *api.ManifestWriter) error {
		log.Trace("manifest updated", "ruid", ruid, "key", addr, "time", time.Now())
		switch contentType {
		case "application/x-tar":
			_, err := s.handleTarUpload(r, mw)
			if err != nil {
				respondError(w, r, fmt.Sprintf("error uploading tarball: %v", err), http.StatusInternalServerError)
				return err
			}
			return nil
		case "multipart/form-data":
			return s.handleMultipartUpload(r, params["boundary"], mw)

		default:
			return s.handleDirectUpload(r, mw)
		}
	})
	if err != nil {
		postFilesFail.Inc(1)
		respondError(w, r, fmt.Sprintf("cannot create manifest: %s", err), http.StatusInternalServerError)
		return
	}

	log.Debug("stored content", "ruid", ruid, "key", newAddr)

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, newAddr)
}

func (s *Server) handleTarUpload(r *http.Request, mw *api.ManifestWriter) (storage.Address, error) {
	log.Trace("handle.tar.upload", "ruid", GetRUID(r.Context()))

	defaultPath := r.URL.Query().Get("defaultpath")

	key, err := s.api.UploadTar(r.Context(), r.Body, GetURI(r.Context()).Path, defaultPath, mw)
	if err != nil {
		return nil, err
	}
	return key, nil
}

func (s *Server) handleMultipartUpload(r *http.Request, boundary string, mw *api.ManifestWriter) error {
	ruid := GetRUID(r.Context())
	log.Trace("handle.multipart.upload", "ruid", ruid)
	mr := multipart.NewReader(r.Body, boundary)
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("error reading multipart form: %s", err)
		}

		var size int64
		var reader io.Reader
		if contentLength := part.Header.Get("Content-Length"); contentLength != "" {
			size, err = strconv.ParseInt(contentLength, 10, 64)
			if err != nil {
				return fmt.Errorf("error parsing multipart content length: %s", err)
			}
			reader = part
		} else {
			// copy the part to a tmp file to get its size
			tmp, err := ioutil.TempFile("", "swarm-multipart")
			if err != nil {
				return err
			}
			defer os.Remove(tmp.Name())
			defer tmp.Close()
			size, err = io.Copy(tmp, part)
			if err != nil {
				return fmt.Errorf("error copying multipart content: %s", err)
			}
			if _, err := tmp.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("error copying multipart content: %s", err)
			}
			reader = tmp
		}

		// add the entry under the path from the request
		name := part.FileName()
		if name == "" {
			name = part.FormName()
		}
		uri := GetURI(r.Context())
		path := path.Join(uri.Path, name)
		entry := &api.ManifestEntry{
			Path:        path,
			ContentType: part.Header.Get("Content-Type"),
			Size:        size,
		}
		log.Debug("adding path to new manifest", "ruid", ruid, "bytes", entry.Size, "path", entry.Path)
		contentKey, err := mw.AddEntry(r.Context(), reader, entry)
		if err != nil {
			return fmt.Errorf("error adding manifest entry from multipart form: %s", err)
		}
		log.Debug("stored content", "ruid", ruid, "key", contentKey)
	}
}

func (s *Server) handleDirectUpload(r *http.Request, mw *api.ManifestWriter) error {
	ruid := GetRUID(r.Context())
	log.Debug("handle.direct.upload", "ruid", ruid)
	key, err := mw.AddEntry(r.Context(), r.Body, &api.ManifestEntry{
		Path:        GetURI(r.Context()).Path,
		ContentType: r.Header.Get("Content-Type"),
		Mode:        0644,
		Size:        r.ContentLength,
	})
	if err != nil {
		return err
	}
	log.Debug("stored content", "ruid", ruid, "key", key)
	return nil
}

// HandleDelete handles a DELETE request to bzz:/<manifest>/<path>, removes
// <path> from <manifest> and returns the resulting manifest hash as a
// text/plain response
func (s *Server) HandleDelete(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.delete", "ruid", ruid)
	deleteCount.Inc(1)
	newKey, err := s.api.Delete(r.Context(), uri.Addr, uri.Path)
	if err != nil {
		deleteFail.Inc(1)
		respondError(w, r, fmt.Sprintf("could not delete from manifest: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, newKey)
}

// Handles feed manifest creation and feed updates
// The POST request admits a JSON structure as defined in the feeds package: `feed.updateRequestJSON`
// The requests can be to a) create a feed manifest, b) update a feed or c) both a+b: create a feed manifest and publish a first update
func (s *Server) HandlePostFeed(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.post.feed", "ruid", ruid)
	var err error

	// Creation and update must send feed.updateRequestJSON JSON structure
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		respondError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	fd, err := s.api.ResolveFeed(r.Context(), uri, r.URL.Query())
	if err != nil { // couldn't parse query string or retrieve manifest
		getFail.Inc(1)
		httpStatus := http.StatusBadRequest
		if err == api.ErrCannotLoadFeedManifest || err == api.ErrCannotResolveFeedURI {
			httpStatus = http.StatusNotFound
		}
		respondError(w, r, fmt.Sprintf("cannot retrieve feed from manifest: %s", err), httpStatus)
		return
	}

	var updateRequest feed.Request
	updateRequest.Feed = *fd
	query := r.URL.Query()

	if err := updateRequest.FromValues(query, body); err != nil { // decodes request from query parameters
		respondError(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	switch {
	case updateRequest.IsUpdate():
		// Verify that the signature is intact and that the signer is authorized
		// to update this feed
		// Check this early, to avoid creating a feed and then not being able to set its first update.
		if err = updateRequest.Verify(); err != nil {
			respondError(w, r, err.Error(), http.StatusForbidden)
			return
		}
		_, err = s.api.FeedsUpdate(r.Context(), &updateRequest)
		if err != nil {
			respondError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		fallthrough
	case query.Get("manifest") == "1":
		// we create a manifest so we can retrieve feed updates with bzz:// later
		// this manifest has a special "feed type" manifest, and saves the
		// feed identification used to retrieve feed updates later
		m, err := s.api.NewFeedManifest(r.Context(), &updateRequest.Feed)
		if err != nil {
			respondError(w, r, fmt.Sprintf("failed to create feed manifest: %v", err), http.StatusInternalServerError)
			return
		}
		// the key to the manifest will be passed back to the client
		// the client can access the feed  directly through its Feed member
		// the manifest key can be set as content in the resolver of the ENS name
		outdata, err := json.Marshal(m)
		if err != nil {
			respondError(w, r, fmt.Sprintf("failed to create json response: %s", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, string(outdata))

		w.Header().Add("Content-type", "application/json")
	default:
		respondError(w, r, "Missing signature in feed update request", http.StatusBadRequest)
	}
}

// HandleGetFeed retrieves Swarm feeds updates:
// bzz-feed://<manifest address or ENS name> - get latest feed update, given a manifest address
// - or -
// specify user + topic (optional), subtopic name (optional) directly, without manifest:
// bzz-feed://?user=0x...&topic=0x...&name=subtopic name
// topic defaults to 0x000... if not specified.
// name defaults to empty string if not specified.
// thus, empty name and topic refers to the user's default feed.
//
// Optional parameters:
// time=xx - get the latest update before time (in epoch seconds)
// hint.time=xx - hint the lookup algorithm looking for updates at around that time
// hint.level=xx - hint the lookup algorithm looking for updates at around this frequency level
// meta=1 - get feed metadata and status information instead of performing a feed query
// NOTE: meta=1 will be deprecated in the near future
func (s *Server) HandleGetFeed(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.get.feed", "ruid", ruid)
	var err error

	fd, err := s.api.ResolveFeed(r.Context(), uri, r.URL.Query())
	if err != nil { // couldn't parse query string or retrieve manifest
		getFail.Inc(1)
		httpStatus := http.StatusBadRequest
		if err == api.ErrCannotLoadFeedManifest || err == api.ErrCannotResolveFeedURI {
			httpStatus = http.StatusNotFound
		}
		respondError(w, r, fmt.Sprintf("cannot retrieve feed information from manifest: %s", err), httpStatus)
		return
	}

	// determine if the query specifies period and version or it is a metadata query
	if r.URL.Query().Get("meta") == "1" {
		unsignedUpdateRequest, err := s.api.FeedsNewRequest(r.Context(), fd)
		if err != nil {
			getFail.Inc(1)
			respondError(w, r, fmt.Sprintf("cannot retrieve feed metadata for feed=%s: %s", fd.Hex(), err), http.StatusNotFound)
			return
		}
		rawResponse, err := unsignedUpdateRequest.MarshalJSON()
		if err != nil {
			respondError(w, r, fmt.Sprintf("cannot encode unsigned feed update request: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Add("Content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, string(rawResponse))
		return
	}

	lookupParams := &feed.Query{Feed: *fd}
	if err = lookupParams.FromValues(r.URL.Query()); err != nil { // parse period, version
		respondError(w, r, fmt.Sprintf("invalid feed update request:%s", err), http.StatusBadRequest)
		return
	}

	data, err := s.api.FeedsLookup(r.Context(), lookupParams)

	// any error from the switch statement will end up here
	if err != nil {
		code, err2 := s.translateFeedError(w, r, "feed lookup fail", err)
		respondError(w, r, err2.Error(), code)
		return
	}

	// All ok, serve the retrieved update
	log.Debug("Found update", "feed", fd.Hex(), "ruid", ruid)
	w.Header().Set("Content-Type", api.MimeOctetStream)
	http.ServeContent(w, r, "", time.Now(), bytes.NewReader(data))
}

func (s *Server) translateFeedError(w http.ResponseWriter, r *http.Request, supErr string, err error) (int, error) {
	code := 0
	defaultErr := fmt.Errorf("%s: %v", supErr, err)
	rsrcErr, ok := err.(*feed.Error)
	if !ok && rsrcErr != nil {
		code = rsrcErr.Code()
	}
	switch code {
	case storage.ErrInvalidValue:
		return http.StatusBadRequest, defaultErr
	case storage.ErrNotFound, storage.ErrNotSynced, storage.ErrNothingToReturn, storage.ErrInit:
		return http.StatusNotFound, defaultErr
	case storage.ErrUnauthorized, storage.ErrInvalidSignature:
		return http.StatusUnauthorized, defaultErr
	case storage.ErrDataOverflow:
		return http.StatusRequestEntityTooLarge, defaultErr
	}

	return http.StatusInternalServerError, defaultErr
}

// HandleGet handles a GET request to
// - bzz-raw://<key> and responds with the raw content stored at the
//   given storage key
// - bzz-hash://<key> and responds with the hash of the content stored
//   at the given storage key as a text/plain response
func (s *Server) HandleGet(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.get", "ruid", ruid, "uri", uri)
	getCount.Inc(1)
	_, pass, _ := r.BasicAuth()

	addr, err := s.api.ResolveURI(r.Context(), uri, pass)
	if err != nil {
		getFail.Inc(1)
		respondError(w, r, fmt.Sprintf("cannot resolve %s: %s", uri.Addr, err), http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "max-age=2147483648, immutable") // url was of type bzz://<hex key>/path, so we are sure it is immutable.

	log.Debug("handle.get: resolved", "ruid", ruid, "key", addr)

	// if path is set, interpret <key> as a manifest and return the
	// raw entry at the given path

	etag := common.Bytes2Hex(addr)
	noneMatchEtag := r.Header.Get("If-None-Match")
	w.Header().Set("ETag", fmt.Sprintf("%q", etag)) // set etag to manifest key or raw entry key.
	if noneMatchEtag != "" {
		if bytes.Equal(storage.Address(common.Hex2Bytes(noneMatchEtag)), addr) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// check the root chunk exists by retrieving the file's size
	reader, isEncrypted := s.api.Retrieve(r.Context(), addr)
	if _, err := reader.Size(r.Context(), nil); err != nil {
		getFail.Inc(1)
		respondError(w, r, fmt.Sprintf("root chunk not found %s: %s", addr, err), http.StatusNotFound)
		return
	}

	w.Header().Set("X-Decrypted", fmt.Sprintf("%v", isEncrypted))

	switch {
	case uri.Raw():
		// allow the request to overwrite the content type using a query
		// parameter
		if typ := r.URL.Query().Get("content_type"); typ != "" {
			w.Header().Set("Content-Type", typ)
		} /*else{
			w.Header().Set("Content-Type", "application/octet-stream")
		}*/
		http.ServeContent(w, r, "", time.Now(), reader)
	case uri.Hash():
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, addr)
	}
}

// HandleGet handles a GET request to
// - chunk://<key> and responds with the raw content stored at the
//   given storage key
func (s *Server) HandleGetChunk(w http.ResponseWriter, r *http.Request) {

	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.getchunk", "ruid", ruid, "uri", uri)
	getCount.Inc(1)
	_, pass, _ := r.BasicAuth()

	addr, err := s.api.ResolveURI(r.Context(), uri, pass)
	if err != nil {
		getFail.Inc(1)
		respondError(w, r, fmt.Sprintf("cannot resolve %s: %s", uri.Addr, err), http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "max-age=2147483648, immutable") // url was of type bzz://<hex key>/path, so we are sure it is immutable.

	log.Debug("handle.get: resolved", "ruid", ruid, "key", addr)

	// if path is set, interpret <key> as a manifest and return the
	// raw entry at the given path

	etag := common.Bytes2Hex(addr)
	noneMatchEtag := r.Header.Get("If-None-Match")
	w.Header().Set("ETag", fmt.Sprintf("%q", etag)) // set etag to manifest key or raw entry key.
	if noneMatchEtag != "" {
		if bytes.Equal(storage.Address(common.Hex2Bytes(noneMatchEtag)), addr) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// check the root chunk exists by retrieving the file's size
	var chunk storage.Chunk
	//var err error
	queries, _ := url.ParseQuery(r.URL.RawQuery)
	if queries.Get("fromws") == "true" {

		chunk, err = s.api.RetrieveChunkFromGS(r.Context(), addr)
	} else {
		chunk, err = s.api.RetrieveChunk(r.Context(), addr)
	}

	if err != nil {
		getFail.Inc(1)
		respondError(w, r, fmt.Sprintf("root chunk not found %s: %s", addr, err), http.StatusNotFound)
		return
	}

	switch {
	case uri.Chunk():
		// allow the request to overwrite the content type using a query
		// parameter
		if typ := r.URL.Query().Get("content_type"); typ != "" {
			w.Header().Set("Content-Type", "application/octet-stream")
		}
		s.chunks++
		w.Write(chunk.Data())

	}
}

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   300 * time.Second,
				KeepAlive: 300 * time.Second,
			}).DialContext,
			MaxIdleConns:        2,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     1000 * time.Second,
		},
	}
	return client
}



// HandleGetM3u8 处理m3u8
// -
//   given storage key
func (s *Server) HandleGetVideoFile(w http.ResponseWriter, r *http.Request) {

	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())

	log.Debug("handle.file", "ruid", ruid, "uri", uri)
	getCount.Inc(1)
	//_, pass, _ := r.BasicAuth()


	pattern_meu8 := regexp.MustCompile(`\/file:\/(?P<schema>https?)\/(?P<path>(\S+\/)+)\{(?P<hash>[0-9a-fA-F]{64})\}(\/(?P<act>(\S*)))`)
	replacedURL := strings.Replace(strings.Replace(r.RequestURI, "%7B", "{", -1), "%7D", "}", -1)
	result := pattern_meu8.FindSubmatch([]byte(replacedURL))
	if len(result) != 0 {
		schema := string(result[1])
		path := schema + "://" + string(result[2])

		hash := result[4]

		act := string(result[6])

		if len(hash) != 0 { //有hash的，说明是要取hash对应的m3u8文件


			fullNodes, _ := s.api.GetNodeCount(r.Context())
			//log.Info("read:","uri",actUri)
			if  fullNodes > 0 {
				//数据片断与哈希的对应关系应该已经存储在数据库里
				newContext := context.WithValue(r.Context(), "url", string(path+act))
				//newContext = context.WithValue(newContext,"server",*s)
				newContext = context.WithValue(newContext, "request", r)

				newContext = context.WithValue(newContext, "reporter", s.httpClient)
				newContext = context.WithValue(newContext, "hash", common.HexToHash(string(hash)))




				/*if s.m3u8.sizelost > 0 && s.m3u8.sizelost <= 10 {
					newContext, _ = context.WithTimeout(newContext, 5*time.Second)
				} else if s.m3u8.sizelost > 10 {
					s.m3u8.sizelost = 0
					newContext, _ = context.WithTimeout(newContext, 20*time.Second)
				} else {
					newContext, _ = context.WithTimeout(newContext, 20*time.Second)
				}
				*/

			//	newCtx,_ := context.WithTimeout(newContext,time.Duration(int64(size/100)*int64(time.Millisecond)))


				s.HandleGetFile(w, r.WithContext(SetURI(newContext,&api.URI{Addr:string(hash),Scheme:"bzz"})))
				return
			}
			//s.m3u8.sizelost++
			//log.Debug("Get from central node:", "uri", actUri)
			s.m3u8.centralRetrived = s.httpClient.GetDataFromCentralServer(path+act, r, w, common.Hex2Bytes(string(hash)), respondError)
			return

		}
	}

	respondError(w, r, "Invalid URL"+r.RequestURI, 400)
	return

}


// HandleGetM3u8 处理m3u8
// -
//   given storage key
func (s *Server) HandleGetM3u8(w http.ResponseWriter, r *http.Request) {

	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())

	log.Debug("handle.m3u8", "ruid", ruid, "uri", uri)
	getCount.Inc(1)
	_, pass, _ := r.BasicAuth()

	//检查是否有.m3u8文件存在
	isM3u8 := strings.Contains(r.RequestURI, ".m3u8")

	pattern_meu8 := regexp.MustCompile(`\/m3u8:\/(?P<schema>https?)\/(?P<path>(\S+\/)+)\{(?P<hash>[0-9a-fA-F]{64})\}(\/(?P<act>(\S*)))`)
	replacedURL := strings.Replace(strings.Replace(r.RequestURI, "%7B", "{", -1), "%7D", "}", -1)
	result := pattern_meu8.FindSubmatch([]byte(replacedURL))
	if len(result) != 0 {
		schema := string(result[1])
		path := schema + "://" + string(result[2])

		hash := result[4]

		act := string(result[6])

		if len(hash) != 0 { //有hash的，说明是要取hash对应的m3u8文件
			url := string(hash) + "/" + act

			s.entries.Add(path, hash)
			actUri, err := api.Parse("m3u8:/" + url)
			fullNodes, _ := s.api.GetNodeCount(r.Context())
			//log.Info("read:","uri",actUri)
			if err == nil && fullNodes > 0 {
				//数据片断与哈希的对应关系应该已经存储在数据库里
				newContext := context.WithValue(r.Context(), "url", string(path+act))
				//newContext = context.WithValue(newContext,"server",*s)
				newContext = context.WithValue(newContext, "request", r)

				newContext = context.WithValue(newContext, "reporter", s.httpClient)
				newContext = context.WithValue(newContext, "hash", common.HexToHash(string(hash)))
				var timeout int32 = 30000
				duration := atomic.LoadInt32(&s.m3u8.cachedDuration)
				if s.m3u8.centralRetrived {
					if duration <= 5000 {
						if fullNodes < 3 {
							timeout = int32(2000 + int(fullNodes)*1000)
						} else {
							timeout = 5000
						}

					} else if duration >= 30000 {
						timeout = 20000
					} else {
						timeout = duration/2 + 5000
					}
				}

				log.Trace("set timeout:", "ms", int64(timeout))

				/*if s.m3u8.sizelost > 0 && s.m3u8.sizelost <= 10 {
					newContext, _ = context.WithTimeout(newContext, 5*time.Second)
				} else if s.m3u8.sizelost > 10 {
					s.m3u8.sizelost = 0
					newContext, _ = context.WithTimeout(newContext, 20*time.Second)
				} else {
					newContext, _ = context.WithTimeout(newContext, 20*time.Second)
				}
				*/
				anewContext, _ := context.WithTimeout(newContext, time.Duration(int64(3000)*int64(time.Millisecond)))
				addr, err := s.api.ResolveURI(anewContext, actUri, pass)
				if err == nil {
					newContext, _ = context.WithTimeout(newContext, time.Duration(int64(timeout)*int64(time.Millisecond)))
					//log.Info("addr resolved:","uri",actUri)
					reader, isEncrypted := s.api.Retrieve(newContext, addr)
					_, err := reader.Size(newContext, nil)
					if err == nil {
						//	log.Info("size ok:","uri",actUri)
						w.Header().Set("X-Decrypted", fmt.Sprintf("%v", isEncrypted))

						if isM3u8 {
							w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
						} else {
							w.Header().Set("Content-Type", "video/MP2T")
						}
						//if typ := r.URL.Query().Get("content_type"); typ != "" {

						//}
						//startServ := time.Now()
						http.ServeContent(w, r, "", time.Now(), reader)
						//	log.Info("served ok:","uri",actUri,"serve time:",time.Now().Sub(startServ))
						/*if time.Now().Sub(startServ) < 10*time.Second {
							s.m3u8.sizelost = 0
						}else{
							s.m3u8.sizelost++
						}*/
						return
					}
				}
			}
			//s.m3u8.sizelost++
			log.Debug("Get from central node:", "uri", actUri)
			s.m3u8.centralRetrived = s.httpClient.GetDataFromCentralServer(path+act, r, w, common.Hex2Bytes(string(hash)), respondError)
			return

		}
	}

	respondError(w, r, "Invalid URL"+r.RequestURI, 400)
	return

}

// HandleGet handles a GET request to
// - chunk://<key> and responds with the raw content stored at the
//   given storage key
func (s *Server) HandleGetMetrics(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.metrics", "ruid", ruid, "uri", uri)
	getCount.Inc(1)

	switch {
	case uri.Metrics():
		// allow the request to overwrite the content type using a query
		// parameter
		w.Header().Set("Content-Type", "application/octet-stream")
		var buf = make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(s.chunks))
		w.Write(buf)

	}
}

// HandleGet handles a GET request to
// - chunk://<key> and responds with the raw content stored at the
//   given storage key
func (s *Server) HandlePostMetrics(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	log.Debug("handle.setmetrics", "ruid", ruid, "uri", uri)
	getCount.Inc(1)
	w.Header().Set("Content-Type", "application/octet-stream")
	s.chunks = 0
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(s.chunks))
	w.Write(buf)
}

// HandleGetList handles a GET request to bzz-list:/<manifest>/<path> and returns
// a list of all files contained in <manifest> under <path> grouped into
// common prefixes using "/" as a delimiter
func (s *Server) HandleGetList(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	_, credentials, _ := r.BasicAuth()
	log.Debug("handle.get.list", "ruid", ruid, "uri", uri)
	getListCount.Inc(1)

	// ensure the root path has a trailing slash so that relative URLs work
	if uri.Path == "" && !strings.HasSuffix(r.URL.Path, "/") {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
		return
	}

	addr, err := s.api.Resolve(r.Context(), uri.Addr)
	if err != nil {
		getListFail.Inc(1)
		respondError(w, r, fmt.Sprintf("cannot resolve %s: %s", uri.Addr, err), http.StatusNotFound)
		return
	}
	log.Debug("handle.get.list: resolved", "ruid", ruid, "key", addr)

	list, err := s.api.GetManifestList(r.Context(), s.api.Decryptor(r.Context(), credentials), addr, uri.Path)
	if err != nil {
		getListFail.Inc(1)
		if isDecryptError(err) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", addr.String()))
			respondError(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		respondError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	// if the client wants HTML (e.g. a browser) then render the list as a
	// HTML index with relative URLs
	if strings.Contains(r.Header.Get("Accept"), "text/html") {
		w.Header().Set("Content-Type", "text/html")
		err := TemplatesMap["bzz-list"].Execute(w, &htmlListData{
			URI: &api.URI{
				Scheme: "bzz",
				Addr:   uri.Addr,
				Path:   uri.Path,
			},
			List: &list,
		})
		if err != nil {
			getListFail.Inc(1)
			log.Error(fmt.Sprintf("error rendering list HTML: %s", err))
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&list)
}

type datacount struct {
	Address common.Address
	Count   int32
}

// map[[20]byte]ReceiptItems -->map[time.Time]ReceiptItem
type receiptInfo struct {
	Address common.Address
	Time    time.Time
	Amount  uint32
	Sign    []byte
}

type receiptResult struct {
	DataFromCentral int64
	Chunks          []datacount
	Receipts        []receiptInfo
}
type NodeCountInfo struct {
	Full    int
	Light   int
	Current int
	Total   int
}

// HandleGetFile handles a GET request to bzz://<manifest>/<path> and responds
// with the content of the file at <path> from the given <manifest>
func (s *Server) HandleGetNodes(w http.ResponseWriter, r *http.Request) {
	ret := NodeCountInfo{0, 0, 0, 0}
	ret.Full, ret.Light = s.api.GetNodeCount(r.Context())
	ret.Current, ret.Total = s.api.GetTransferStatus(r.Context())
	json.NewEncoder(w).Encode(&ret)
}

// HandleGetFile handles a GET request to bzz://<manifest>/<path> and responds
// with the content of the file at <path> from the given <manifest>
func (s *Server) HandleGetReceived(w http.ResponseWriter, r *http.Request) {
	result, err := s.api.GetReadCount(r.Context())
	if err != nil {
		respondError(w, r, err.Error(), http.StatusInternalServerError)
	} else {
		w.Header().Set("Content-Type", "application/json")
		ret := receiptResult{
			s.httpClient.GetDataLenFromCenter(),
			make([]datacount, 0),
			make([]receiptInfo, 0),
		}
		for addr, count := range result.ReceivedChunks {
			ret.Chunks = append(ret.Chunks, datacount{addr, int32(count)})
		}
		for _, receipt := range result.Receipts {
			for addr, items := range receipt {
				for _time, item := range items {
					ret.Receipts = append(ret.Receipts, receiptInfo{common.Address(addr), _time, item.Amount, item.Sign})
				}
			}
		}

		json.NewEncoder(w).Encode(&ret)
	}
}

// HandleGetFile handles a GET request to bzz://<manifest>/<path> and responds
// with the content of the file at <path> from the given <manifest>
func (s *Server) HandleGetFile(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	uri := GetURI(r.Context())
	_, credentials, _ := r.BasicAuth()
	log.Debug("handle.get.file", "ruid", ruid, "uri", r.RequestURI)
	getFileCount.Inc(1)

	// ensure the root path has a trailing slash so that relative URLs work
	if uri.Path == "" && !strings.HasSuffix(r.URL.Path, "/") {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
		return
	}
	var err error
	manifestAddr := uri.Address()

	if manifestAddr == nil {
		manifestAddr, err = s.api.Resolve(r.Context(), uri.Addr)
		if err != nil {
			getFileFail.Inc(1)
			respondError(w, r, fmt.Sprintf("cannot resolve %s: %s", uri.Addr, err), http.StatusNotFound)
			return
		}
	} else {
		w.Header().Set("Cache-Control", "max-age=2147483648, immutable") // url was of type bzz://<hex key>/path, so we are sure it is immutable.
	}

	log.Debug("handle.get.file: resolved", "ruid", ruid, "key", manifestAddr)

	reader, contentType, status, contentKey, err := s.api.Get(r.Context(), s.api.Decryptor(r.Context(), credentials), manifestAddr, uri.Path)

	etag := common.Bytes2Hex(contentKey)
	noneMatchEtag := r.Header.Get("If-None-Match")
	w.Header().Set("ETag", fmt.Sprintf("%q", etag)) // set etag to actual content key.
	if noneMatchEtag != "" {
		if bytes.Equal(storage.Address(common.Hex2Bytes(noneMatchEtag)), contentKey) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	if err != nil {
		if isDecryptError(err) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", manifestAddr))
			respondError(w, r, err.Error(), http.StatusUnauthorized)
			return
		}

		switch status {
		case http.StatusNotFound:
			getFileNotFound.Inc(1)
			respondError(w, r, err.Error(), http.StatusNotFound)
		default:
			getFileFail.Inc(1)
			respondError(w, r, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	//the request results in ambiguous files
	//e.g. /read with readme.md and readinglist.txt available in manifest
	if status == http.StatusMultipleChoices {
		list, err := s.api.GetManifestList(r.Context(), s.api.Decryptor(r.Context(), credentials), manifestAddr, uri.Path)
		if err != nil {
			getFileFail.Inc(1)
			if isDecryptError(err) {
				w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=%q", manifestAddr))
				respondError(w, r, err.Error(), http.StatusUnauthorized)
				return
			}
			respondError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Debug(fmt.Sprintf("Multiple choices! --> %v", list), "ruid", ruid)
		//show a nice page links to available entries
		ShowMultipleChoices(w, r, list)
		return
	}

	// check the root chunk exists by retrieving the file's size
	if _, err := reader.Size(r.Context(), nil); err != nil {
		getFileNotFound.Inc(1)
		respondError(w, r, fmt.Sprintf("file not found %s: %s", uri, err), http.StatusNotFound)
		return
	}

	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	fileName := uri.Addr
	if found := path.Base(uri.Path); found != "" && found != "." && found != "/" {
		fileName = found
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", fileName))

	http.ServeContent(w, r, fileName, time.Now(), newBufferedReadSeeker(reader, getFileBufferSize))
}

// HandleGetFile handles a GET request to bzz://<manifest>/<path> and responds
// with the content of the file at <path> from the given <manifest>
func (s *Server) HandleDuration(w http.ResponseWriter, r *http.Request) {
	ruid := GetRUID(r.Context())
	log.Debug("handle.post.raw", "ruid", ruid)

	postRawCount.Inc(1)

	//toEncrypt := false

	durationBuf, err := ioutil.ReadAll(r.Body)
	if err == nil {
		value, err := strconv.Atoi(string(durationBuf))
		if err == nil {
			atomic.StoreInt32(&s.m3u8.cachedDuration, int32(value))
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	respondError(w, r, fmt.Sprintf("error in resolve duration: %s", err), http.StatusBadRequest)

}

// The size of buffer used for bufio.Reader on LazyChunkReader passed to
// http.ServeContent in HandleGetFile.
// Warning: This value influences the number of chunk requests and chunker join goroutines
// per file request.
// Recommended value is 4 times the io.Copy default buffer value which is 32kB.
const getFileBufferSize = 4 * 32 * 1024

// bufferedReadSeeker wraps bufio.Reader to expose Seek method
// from the provied io.ReadSeeker in newBufferedReadSeeker.
type bufferedReadSeeker struct {
	r io.Reader
	s io.Seeker
}

// newBufferedReadSeeker creates a new instance of bufferedReadSeeker,
// out of io.ReadSeeker. Argument `size` is the size of the read buffer.
func newBufferedReadSeeker(readSeeker io.ReadSeeker, size int) bufferedReadSeeker {
	return bufferedReadSeeker{
		r: bufio.NewReaderSize(readSeeker, size),
		s: readSeeker,
	}
}

func (b bufferedReadSeeker) Read(p []byte) (n int, err error) {
	return b.r.Read(p)
}

func (b bufferedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return b.s.Seek(offset, whence)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func isDecryptError(err error) bool {
	return strings.Contains(err.Error(), api.ErrDecrypt.Error())
}
