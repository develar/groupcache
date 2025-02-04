/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
    "context"
    "fmt"
    "github.com/develar/groupcache/consistent"
    "io"
    "net/http"
    "strconv"
    "strings"
    "time"

    "github.com/valyala/bytebufferpool"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	peers *consistent.Consistent
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, uses the http.Request.Context()
	Context func(*http.Request) context.Context
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

    basePath := defaultBasePath
    if o != nil && o.BasePath != "" {
        basePath = o.BasePath
    }

	p := &HTTPPool{
		self: self + basePath,
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistent.New(nil, createConsistentConfig(p.opts.Replicas))

	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

func createConsistentConfig(replicationFactor int) consistent.Config {
	return consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Logger:            logger,
	}
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peerUrls ...string) {
	peers := make([]consistent.Member, len(peerUrls))
	for index, peerUrl := range peerUrls {
		peers[index] = &httpGetter{
			getTransport: p.opts.Transport,
			url:          peerUrl + p.opts.BasePath,
		}
	}
	p.peers.Set(peers)
}

// GetAll returns all the peers in the pool
func (p *HTTPPool) GetAll() []ProtoGetter {
	var i int
	members := p.peers.GetMembers()
	result := make([]ProtoGetter, len(members))
	for _, v := range members {
		result[i] = v.(*httpGetter)
		i++
	}
	return result
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	peer := p.peers.LocateKey([]byte(key))
	if peer != nil && peer.String() != p.self {
		peerImpl, ok := peer.(*httpGetter)
		if !ok {
			panic("cannot cast to httpGetter")
		}
		return peerImpl, true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

    groupName := r.Header[headerGroup][0]
   	key := r.Header[headerKey][0]
	if groupName == "" || key == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.opts.Context != nil {
		ctx = p.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	group.Stats.ServerRequests.Add(1)

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.localRemove(key)
		return
	}

    value, expire, err := group.GetWithExpire(ctx, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

    if !expire.IsZero() {
        w.Header()[headerExpire] = []string{strconv.FormatUint(uint64(expire.UnixMicro()), 10)}
    }
    w.Header()["Content-Type"] = []string{"application/x-protobuf"}

    buffer := bytebufferpool.Get()
    defer bytebufferpool.Put(buffer)

    size := value.SizeVT()
    if cap(buffer.B) < size {
        buffer.B = make([]byte, size*2)
    }

    n, err := value.MarshalToSizedBufferVT(buffer.B[0:size])
    if err != nil {
   		http.Error(w, err.Error(), http.StatusInternalServerError)
   		return
   	}
    w.Header()["Content-Length"] = []string{strconv.Itoa(n)}
    _, _ = w.Write(buffer.B[0:n])
}

const headerExpire = "X-Expire"
const headerGroup = "X-Group"
const headerKey = "X-Key"

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	url          string
}

// GetURL
func (h *httpGetter) GetURL() string {
	return h.url
}

func (h httpGetter) String() string {
	return h.url
}

func (h *httpGetter) makeRequest(ctx context.Context, method string, group string, key string, out *http.Response) error {
	req, err := http.NewRequestWithContext(ctx, method, h.url, nil)
    req.Header.Set(headerGroup, group)
    req.Header.Set(headerKey, key)
	if err != nil {
		return err
	}

	tr := http.DefaultTransport
	if h.getTransport != nil {
		tr = h.getTransport(ctx)
	}

	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	*out = *res
	return nil
}

func (h *httpGetter) Get(ctx context.Context, group string, key string, valueAllocator ValueAllocator) (Value, time.Time, error) {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, group, key, &res); err != nil {
		return nil, time.Time{}, err
	}
	defer res.Body.Close()

    var expire time.Time
	if res.StatusCode != http.StatusOK {
		return nil, expire, fmt.Errorf("server returned: %v", res.Status)
	}

    buffer := bytebufferpool.Get()
    defer bytebufferpool.Put(buffer)
    _, err := buffer.ReadFrom(res.Body)
    if err != nil {
        return nil, expire, fmt.Errorf("reading response body: %v", err)
    }

    if err != nil {
        return nil, expire, err
    }

    expireString := res.Header.Get(headerExpire)
    if expireString != "" {
        expireMilli, err := strconv.ParseUint(expireString, 10, 64)
        if err != nil {
            return nil, expire, fmt.Errorf("reading response body: %v", err)
        }
        expire = time.UnixMicro(int64(expireMilli))
    }

    value := valueAllocator()
    err = value.UnmarshalVT(buffer.B)
    if err != nil {
        return nil, expire, err
    }
    return value, expire, nil
}

func readAllBytes(body io.ReadCloser) ([]byte, error) {
    b := bytebufferpool.Get()
    defer bytebufferpool.Put(b)
    _, err := b.ReadFrom(body)
    if err != nil {
        return nil, err
    }

    data := make([]byte, b.Len())
    copy(data, b.B)
    return data, err
}

func (h *httpGetter) Remove(ctx context.Context, group string, key string) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, group, key, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := readAllBytes(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}
