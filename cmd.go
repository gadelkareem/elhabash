package elhabash

import (
	"net/http"
	"net/url"
)

// Command interface defines the methods required by the Fetcher to request
// a resource.
type Command interface {
	Url() *url.URL
	MirrorUrl() *url.URL
	ReferrerUrl() *url.URL
	Method() string
	HttpClient() *Client
	SetHttpClient(client *Client)
	isDisableMirror() bool
	isDisableCache() bool
	DisableCache()
}

type Client struct {
	http.Client
	ProxyUrl        *url.URL
	ProxyOutboundIp string
	UserAgent       string
	Mirror          string
}

// Cmd defines a basic Command implementation.
type Cmd struct {
	U             *url.URL
	M             string
	C             *Client
	DisableMirror bool
	disableCache  bool
	Referrer      *url.URL
}

// Url returns the resource targeted by this command.
func (c *Cmd) Url() *url.URL {
	return c.U
}

func (c *Cmd) ReferrerUrl() *url.URL {
	if c.Referrer == nil {
		return nil
	}
	u := new(url.URL)
	*u = *c.Referrer
	return u
}

func (c *Cmd) MirrorUrl() *url.URL {
	u := new(url.URL)
	*u = *c.U
	if !c.DisableMirror && c.C != nil && c.C.Mirror != "" {
		u.Host = c.C.Mirror
	}
	return u
}

// Method returns the HTTP verb to use to process this command (i.e. "GET", "HEAD", etc.).
func (c *Cmd) Method() string {
	return c.M
}

func (c *Cmd) isDisableMirror() bool {
	return c.DisableMirror
}

func (c *Cmd) isDisableCache() bool {
	return c.disableCache
}

func (c *Cmd) DisableCache() {
	c.disableCache = true
}

func (c *Cmd) HttpClient() *Client {
	return c.C
}

func (c *Cmd) SetHttpClient(client *Client) {
	c.C = client
}
