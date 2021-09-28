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
    "bytes"
    "context"
    "fmt"
    "github.com/develar/groupcache/consistent"
    "io"
    "io/ioutil"
    "net/http"
    "net/url"
    "strings"
    "sync"

    pb "github.com/develar/groupcache/groupcachepb"
    "github.com/golang/protobuf/proto"
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
		var ok bool
		result[i], ok = v.(*httpGetter)
		if !ok {
			panic("cannot cast to httpGetter")
		}
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
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
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

	var b []byte

	value := AllocatingByteSliceSink(&b)
	err := group.Get(ctx, key, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view, err := value.view()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var expireNano int64
	if !view.e.IsZero() {
		expireNano = view.Expire().UnixNano()
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: b, Expire: &expireNano})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	url          string
}

// GetURL
func (p *httpGetter) GetURL() string {
	return p.url
}

func (p httpGetter) String() string {
	return p.url
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpGetter) makeRequest(ctx context.Context, method string, in *pb.GetRequest, out *http.Response) error {
	u := h.url + url.QueryEscape(in.GetGroup()) + "/" + url.QueryEscape(in.GetKey())
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
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

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err := io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (h *httpGetter) Remove(ctx context.Context, in *pb.GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}
