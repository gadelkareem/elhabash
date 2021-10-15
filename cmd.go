package elhabash

import (
    "net/http"
    "net/url"
    "strings"

    "github.com/gadelkareem/go-helpers"
)

// Command interface defines the methods required by the Fetcher to request
// a resource.
type Command interface {
    Url() *url.URL
    MirrorUrl() *url.URL
    ReferrerUrl() *url.URL
    Method() string
    HttpClient() *Client
    Request() *http.Request
    SetHttpClient(client *Client) Command
    SetRequest(r *http.Request) Command
    isDisableMirror() bool
    isDisableCache() bool
}

type Client struct {
    http.Client
    ProxyURL        *url.URL
    ProxyOutboundIp string
    DirectProxyURL  string
    UserAgent       string
    Mirror          string
}

func (cl *Client) UpdateDirectProxyURL() {
    pu := h.ParseUrl(cl.ProxyURL.String())
    port := pu.Port()
    if strings.Contains(cl.ProxyOutboundIp, ":") {
        pu.Host = "[" + cl.ProxyOutboundIp + "]"
    } else {
        pu.Host = cl.ProxyOutboundIp
    }
    pu.Host += ":" + port
    cl.DirectProxyURL = pu.String()
}

// Cmd defines a basic Command implementation.
type Cmd struct {
    U             *url.URL
    M             string
    C             *Client
    R             *http.Request
    DisableMirror bool
    DisableCache  bool
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
    return c.DisableCache
}

func (c *Cmd) Request() *http.Request {
    return c.R
}

func (c *Cmd) HttpClient() *Client {
    return c.C
}

func (c *Cmd) SetHttpClient(client *Client) Command {
    c.C = client
    return c
}

func (c *Cmd) SetRequest(r *http.Request) Command {
    c.R = r
    c.U = r.URL
    c.M = r.Method
    return c
}
