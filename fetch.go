package elhabash

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/gadelkareem/cachita"
	"github.com/gadelkareem/faloota"
	"github.com/gadelkareem/quiver"
	"golang.org/x/text/encoding/htmlindex"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/astaxie/beego/logs"
	"github.com/gadelkareem/go-helpers"
	"github.com/temoto/robotstxt"
)

var (
	// ErrEmptyHost is returned if a command to be enqueued has an Url with an empty host.
	ErrEmptyHost = errors.New("invalid empty host")

	// ErrDisallowed is returned when the requested Url is disallowed by the robots.txt
	// policy.
	ErrDisallowed = errors.New("disallowed by robots.txt")

	// ErrQueueClosed is returned when a Send call is made on a closed Queue.
	ErrQueueClosed = errors.New("send on a closed queue")
)

// Parse the robots.txt relative path a single time at startup, this can't
// return an error.
var robotsTxtParsedPath, _ = url.Parse("/robots.txt")

const (
	// DefaultCrawlDelay represents the delay to use if there is no robots.txt
	// specified delay.
	DefaultCrawlDelay = 5 * time.Second

	// DefaultUserAgent is the default user agent string.
	GoogleBotUserAgent = "Googlebot/2.1 (+http://www.google.com/bot.html)"

	// DefaultWorkerIdleTTL is the default time-to-live of an idle host worker goroutine.
	// If no Url is sent for a given host within this duration, this host's goroutine
	// is disposed of.
	DefaultWorkerIdleTTL = 5 * time.Second

	MaxAllowedGoRoutines = 10000

	//Max number of commands for client before creating a new one - with a new ProxyFactory
	MaximumClientCommands = 300

	//Max number of Url errors
	MaxUrlErrors    = 10
	MaxMirrorErrors = 500

	ClientTimeout = 30
)

// A HandlerFunc is a function signature that implements the Handler interface. A function
// with this signature can thus be used as a Handler.
type HandlerFunc func(*goquery.Document, Command, *http.Response, error)

// A Fetcher defines the parameters for running a web crawler.
type Fetcher struct {
	// The Handler to be called for each request. All successfully enqueued requests
	// produce a Handler call.
	Handler HandlerFunc

	// encoding string as in http://www.w3.org/TR/encoding
	DecodeCharset string
	// DisablePoliteness disables fetching and using the robots.txt policies of
	// channels.
	DisablePoliteness bool

	// Default delay to use between requests to a same host if there is no robots.txt
	// crawl delay or if DisablePoliteness is true.
	CrawlDelay time.Duration

	// The time a host-dedicated worker goroutine can stay idle, with no Command to enqueue,
	// before it is stopped and cleared from memory.
	WorkerIdleTTL time.Duration

	MaxWorkers       int
	workersWaitGroup sync.WaitGroup

	// queue holds the Queue to send data to the fetcher and optionally close (stop) it.
	queue        *Queue
	ShuttingDown bool

	//used mainly to display emoiji instead of debug info
	LogLevel int

	// channels maps the host names to its dedicated requests channel, and channelsMu protects
	// concurrent access to the channels field.
	channels []chan Command

	ProxyFactory quiver.ProxyFactory

	Faloota       *faloota.Faloota
	FalootaVerify faloota.Action

	Headers          map[string]string
	cookiesMu        sync.RWMutex
	cookies          []*http.Cookie
	DisableKeepAlive bool

	mirrorsMu            sync.Mutex
	Mirrors              []string
	DisableMirrorTesting bool
	MainHost             string
	Name                 string

	MaxCommandsPerClient int

	// Protect access to UrlErrors
	urlErrorsMu sync.Mutex
	// errors table
	urlErrors map[string]map[string]int

	sleepingMu        sync.Mutex
	sleepingProcesses int

	//exclude filter is higher than the include filter
	ExcludeRegexFilter *regexp.Regexp
	IncludeRegexFilter *regexp.Regexp
	StopString         string

	KeepCrawling bool

	Cache          cachita.Cache
	CacheQueueSize int
	cacheQueueMu   sync.Mutex
	cacheQueue     []*Page

	startTime time.Time

	robotAgentsMu sync.Mutex
	robotAgents   map[string]*robotstxt.Group

	DisableSnapshot bool
	snapshot        *Snapshot
}

// Start starts the Fetcher, and returns the Queue to use to send Commands to be fetched.
func (f *Fetcher) Start(rawUrl string) *Queue {

	rLimit, _ := h.LiftRLimits()
	if rLimit.Cur < 2000 {
		panic("you need to increase file descriptors...")
	}
	logs.Alert("rlimit final %v", rLimit)

	if f.MainHost == "" {
		panic("no MainHost specified...")
	}

	if f.Faloota != nil && f.FalootaVerify == nil {
		panic("no verify func found for faloota")
	}

	f.startTime = time.Now()

	f.robotAgents = make(map[string]*robotstxt.Group)
	f.urlErrors = make(map[string]map[string]int)
	f.urlErrors["urls"] = make(map[string]int)
	f.urlErrors["mirrors"] = make(map[string]int)

	f.queue = &Queue{
		commandsChannel: make(chan Command, 1),
		closed:          make(chan struct{}),
		cancelled:       make(chan struct{}),
		done:            make(chan struct{}),
	}

	if f.CrawlDelay <= 0 {
		f.CrawlDelay = DefaultCrawlDelay
	}
	if f.WorkerIdleTTL <= 0 {
		f.WorkerIdleTTL = DefaultWorkerIdleTTL
	}
	if f.MaxCommandsPerClient <= 0 {
		f.MaxCommandsPerClient = MaximumClientCommands
	}

	if f.ProxyFactory != nil {
		if f.MaxWorkers <= 0 {
			proxiesCount := f.ProxyFactory.TotalCount()
			if proxiesCount > 0 && proxiesCount < MaxAllowedGoRoutines {
				f.MaxWorkers = proxiesCount
			} else if proxiesCount > MaxAllowedGoRoutines {
				f.MaxWorkers = MaxAllowedGoRoutines
			} else {
				f.MaxWorkers = 1
			}
		}
	}

	logs.Alert("Using %d goroutines.", f.MaxWorkers)

	f.snapshot = NewSnapshot(f.queue, f.Name, f.DisableSnapshot)

	f.testMirrors()

	// Start the one and only queue processing goroutine.
	f.queue.wg.Add(1)
	go f.processQueue()

	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		go f.exception(err.Error())
		return nil
	}

	f.ensureGracefulShutdown()

	// Enqueue the seed, which is the first entry in the duplicates map
	return f.Get(parsedUrl)
}

func (f *Fetcher) ensureGracefulShutdown() {
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	go func() {
		sig := <-gracefulStop
		logs.Emergency("Caught sig: %+v, shutting down gracefully..", sig)
		logs.Emergency("Break again to force exit..")
		signal.Reset()
		f.Shutdown()
	}()
}

func (f *Fetcher) exception(e string) {
	select {
	case <-f.queue.cancelled:
		// already cancelled, no-op
		return
	default:
	}

	_, err := fmt.Fprintf(os.Stderr, "EXCEPTION: %s", e)
	h.LogOnError(err)

	f.Shutdown()
	debug.PrintStack()
	panic(e)
}

func (f *Fetcher) Shutdown() {
	if f.ShuttingDown {
		return
	}
	f.ShuttingDown = true
	logs.Emergency("Shutting down while queue has %d commands...", f.snapshot.queueLength())
	f.cacheQueueMu.Lock()
	if f.Cache != nil && len(f.cacheQueue) > 0 {
		logs.Alert("Flushing cached URLs %d", len(f.cacheQueue))
		f.cachePutMulti()
	}
	f.cacheQueueMu.Unlock()

	h.LogOnError(f.queue.Cancel())
}

func (f *Fetcher) cachePutMulti() {
	for _, page := range f.cacheQueue {
		err := f.Cache.Put(page.Id, page, 0)
		if err != nil {
			logs.Error("🔥 Error setting cache: %v", err)
		}
	}
}

// processQueue runs the queue in its own goroutine.
func (f *Fetcher) processQueue() {
	var i int

	logs.Info("Launching %d workers", f.MaxWorkers)
	for i = 0; i < f.MaxWorkers; i++ {
		// Create the infinite queue: the in channel to send on, and the out channel
		// to read from in the host's goroutine, and add to the channels map
		inChanCommands, outChanCommands := make(chan Command, 1), make(chan Command, 1)

		f.channels = append(f.channels, inChanCommands)

		// Start the infinite queue goroutine for this host
		go sliceIQ(inChanCommands, outChanCommands)
		// Start the working goroutine for this host
		f.workersWaitGroup.Add(1)
		go f.processChan(outChanCommands, i)
	}

	i = 0
loop:
	for command := range f.queue.commandsChannel {

		if command == nil {
			// Special case, when the Queue is closed, a nil command is sent, use this
			// indicator to check for the closed signal, instead of looking on every loop.
			select {
			case <-f.queue.closed:
				logs.Info("Got close signal on main queue")
				// Close signal, exit loop
				break loop
			default:
				// Keep going
			}
		}
		select {
		case <-f.queue.cancelled:
			logs.Info("Got cancel signal on main queue")
			// queue got stop, drain
			continue
		default:
			// go on
		}

		f.snapshot.addCommandInQueue(f.uniqueId(command.Url(), command.Method()), command)

		// Send the request
		//logs.Debug("New command for Url: %s", command.Url())
		f.channels[i] <- command
		i++
		if i == len(f.channels) {
			i = 0
		}

	}

	for _, ch := range f.channels {
		close(ch)
		for range ch {
		}
	}

	f.workersWaitGroup.Wait()
	f.queue.wg.Done()
	logs.Alert("All commands completed in %s", time.Since(f.startTime))
}

// Goroutine for a host's worker, processing requests for all its URLs.
func (f *Fetcher) processChan(outChanCommands <-chan Command, routineIndex int) {
	var (
		agent *robotstxt.Group
		ttl   <-chan time.Time
		//add some random seconds for each channel
		delay              = f.CrawlDelay
		httpClient         = f.NewClient(true, true)
		httpClientCommands = 0
		restartClient      = false
	)

	//tata tata
	logs.Info("Channel %d is starting..", routineIndex)
	if routineIndex < 10 {
		time.Sleep(time.Duration(routineIndex) + 2*time.Second)
	}

loop:
	for {
		select {
		case <-f.queue.cancelled:
			break loop
		case command, ok := <-outChanCommands:
			if !ok {
				logs.Info("Channel %d was closed, terminating..", routineIndex)
				// Terminate this goroutine, channel is closed
				break loop
			}

			// was it stop during the wait? check again
			select {
			case <-f.queue.cancelled:
				logs.Info("Channel %d was cancelled, terminating..", routineIndex)
				break loop
			default:
				// go on
			}

			restartClient = false

			logs.Debug("Channel %d received new command %s", routineIndex, command.Url())

			if !f.DisablePoliteness {
				agent = f.getRobotAgent(command.Url())
				// Initialize the crawl delay
				if agent != nil && agent.CrawlDelay > 0 {
					delay = agent.CrawlDelay
				}
			}

			if command.HttpClient() == nil {
				command.SetHttpClient(httpClient)
			}

			if f.DisablePoliteness || agent == nil || agent.Test(command.Url().Path) {

				// path allowed, process the request
				res, err, isCached := f.Request(command, delay)

				if !isCached {
					httpClientCommands++

					var statusCode int
					if res != nil {
						statusCode = res.StatusCode
					}
					logs.Info("[%d][%d] %s %s", routineIndex, statusCode, command.Method(), command.Url())
				}

				restartClient = f.visit(command, res, err, isCached)

			} else {
				// path disallowed by robots.txt
				f.visit(command, nil, ErrDisallowed, false)
			}

			f.snapshot.removeCommandInQueue(f.uniqueId(command.Url(), command.Method()))

			// Every time a command is received, reset the ttl channel
			ttl = time.After(f.WorkerIdleTTL)

			if restartClient || httpClientCommands > f.MaxCommandsPerClient {
				if f.LogLevel < logs.LevelNotice {
					fmt.Print("👅")
				}
				logs.Info("Channel %d needs restart after %d commands..", routineIndex, httpClientCommands)
				go f.processChan(outChanCommands, routineIndex)
				return
			}

		case <-ttl:
			if f.snapshot.queueLength() != 0 {
				logs.Debug("Channel %d was trying to timeout while queue length is %d", routineIndex, f.snapshot.queueLength())
				ttl = time.After(f.WorkerIdleTTL)
				continue
			}
			logs.Alert("Channel %d with %d unique urls", routineIndex, f.snapshot.uniqueUrlsLength())
			go f.Shutdown()
			break loop
		}
	}
	for range outChanCommands {
	}

	f.workersWaitGroup.Done()
}

// Prepare and execute the request for this Command.
func (f *Fetcher) Request(cmd Command, delay time.Duration) (response *http.Response, err error, isCached bool) {
	var (
		cacheId string
	)

	isCached = false

	httpClient := cmd.HttpClient()
	if httpClient == nil {
		go f.exception("No HTTP Client in the command:" + cmd.MirrorUrl().String())
		return
	}

	req := cmd.Request()
	if req == nil {
		req, err = http.NewRequest(cmd.Method(), cmd.MirrorUrl().String(), nil)
		if err != nil {
			return
		}
	}
	cmd.SetRequest(req)
	rawUrl := cmd.MirrorUrl().String()

	forceUpdateCache := false
	if cmd.Method() == http.MethodGet {
		cacheId = f.cacheId(cmd.Url(), cmd.Method())
		if cacheId != "" {
			response = f.getCachedResponse(cacheId)
			if response != nil {
				isCached = true
				response.Request = req
				if f.LogLevel < logs.LevelNotice {
					fmt.Print("➰")
				}
				logs.Info("Got cache for Url: %s cacheId: %s", rawUrl, cacheId)
				return
			}
			forceUpdateCache = true
		}
		logs.Info("No cache found for Url: %s cacheId: %s", rawUrl, cacheId)
	}

	if f.DisableKeepAlive {
		req.Close = true
	}

	if cmd.ReferrerUrl() != nil {
		referrer := cmd.ReferrerUrl()
		referrer.Scheme = cmd.Url().Scheme
		logs.Debug("Setting referrer %s for %s", referrer, cmd.Url())
		req.Header.Set("Referer", referrer.String())
	}

	f.passCookies(httpClient, cmd)

	f.sleeping(false)
	logs.Info("Taking a nap for %s", delay)
	time.Sleep(delay)
	f.sleeping(true)

	for k, v := range f.Headers {
		req.Header.Set(k, v)
	}

	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", cmd.HttpClient().UserAgent)
	}

	if cmd.Url().Scheme == "http" && req.Header.Get("Request-IP") == "" && cmd.HttpClient().ProxyOutboundIp != "" {
		req.Header.Set("Request-IP", cmd.HttpClient().ProxyOutboundIp)
		logs.Debug("Using proxy:%s", cmd.HttpClient().ProxyOutboundIp)
	}

	// Do the request.
	response, err = cmd.HttpClient().Do(req)
	if err != nil {
		return
	}

	if response != nil && cmd.Method() == http.MethodGet {
		if response.StatusCode == http.StatusOK ||
			(httpClient.Mirror == "" && //if we ll cache a redirect or not found then make sure it's not a mirror
				(response.StatusCode == http.StatusMovedPermanently || response.StatusCode == http.StatusNotFound)) {
			if cacheId != "" {
				response.Request = req
				f.cacheResponse(cmd, cacheId, response, forceUpdateCache)
				logs.Info("Cached Url: %s cacheId: %s", rawUrl, cacheId)
			}
			if f.LogLevel < logs.LevelNotice {
				fmt.Print("🚀")
			}
		}
	}

	return
}

// Call the Handler for this Command. Closes the response's body.
func (f *Fetcher) visit(cmd Command, response *http.Response, err error, isCached bool) (restartClient bool) {
	restartClient = false

	defer func() {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
	}()
	hasErrors, brokenMirror, brokenUrl := f.HandleError(response, err, cmd)
	if hasErrors {
		logs.Debug("🔥 Error with Url: %s proxy: %s Error: %v", cmd.MirrorUrl(), cmd.HttpClient().ProxyUrl, err)
		if brokenMirror {
			restartClient = true
		}
		if brokenUrl { //too many failures for this Url
			if err != nil && err != ErrDisallowed {
				logs.Error("🔥 Url %s keeps failing, Error: %v", cmd.Url(), err)
			}
			return
		} else if f.LogLevel < logs.LevelNotice {
			fmt.Print("🔥")
		}
		if !isCached {
			cmd.SetHttpClient(nil) //make it light for the queue
			f.queue.Send(cmd)
		}
		return
	}

	var document *goquery.Document
	if f.KeepCrawling && strings.Contains(response.Header.Get("content-type"), "text") {
		logs.Info("Parsing Url %s", cmd.Url())
		var body io.Reader
		if f.DecodeCharset != "" && f.DecodeCharset != "utf-8" {
			e, err := htmlindex.Get(f.DecodeCharset)
			if err != nil {
				go f.exception("Could not find decoding charset " + f.DecodeCharset + " " + err.Error() + " " + cmd.Url().String())
				return
			}
			body = e.NewDecoder().Reader(response.Body)
		} else {
			body = response.Body
		}
		document, err = goquery.NewDocumentFromReader(body)
		if err != nil {
			logs.Error("Error parsing html %s %s - %s", cmd.Method(), cmd.Url(), err)
			return
		}
		f.enqueueLinks(cmd.MirrorUrl(), document)
	}

	logs.Info("Passing to handler Url %s", cmd.Url())
	if f.Handler != nil {
		f.Handler(document, cmd, response, err)
	}

	return

}

func (f *Fetcher) NewClient(withProxy, withMirror bool) *Client {
	var err error

	httpClient := &Client{}

	httpClient.Jar, err = cookiejar.New(nil)
	if err != nil {
		logs.Error("Error setting cookie jar : %s", err)
	}

	if withProxy && f.ProxyFactory != nil {
		httpClient.ProxyOutboundIp, httpClient.ProxyUrl = f.ProxyFactory.RandomProxy()
		if httpClient.ProxyOutboundIp == "" && httpClient.ProxyUrl == nil {
			go f.exception("Running out of proxies..")
			return nil
		}
	}

	keepAlive := ClientTimeout * 4 * time.Second
	idleConnTimeout := ClientTimeout * 4 * time.Second
	maxIdleConns := 100
	if f.DisableKeepAlive {
		keepAlive = 0
		idleConnTimeout = 0
		maxIdleConns = 1
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Dial: (&net.Dialer{
			Timeout:       ClientTimeout * time.Second,
			KeepAlive:     keepAlive,
			FallbackDelay: -1,
		}).Dial,
		TLSHandshakeTimeout:   ClientTimeout * time.Second,
		ResponseHeaderTimeout: ClientTimeout * time.Second,
		IdleConnTimeout:       idleConnTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		DisableKeepAlives:     f.DisableKeepAlive,
		DisableCompression:    false,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConns,
	}
	httpClient.Timeout = ClientTimeout * 2 * time.Second

	proxyUrl := ""
	if httpClient.ProxyUrl != nil {
		transport.Proxy = http.ProxyURL(httpClient.ProxyUrl)
		if httpClient.ProxyOutboundIp != "" {
			transport.ProxyConnectHeader = http.Header{"Request-IP": []string{httpClient.ProxyOutboundIp}}
		}
		proxyUrl = httpClient.ProxyUrl.String()
	}
	httpClient.Transport = transport
	httpClient.UserAgent = userAgents[h.RandomNumber(0, len(userAgents))]
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	host := f.MainHost
	if withMirror && len(f.Mirrors) > 0 {
		f.mirrorsMu.Lock()
		defer f.mirrorsMu.Unlock()
		httpClient.Mirror = f.Mirrors[h.RandomNumber(0, len(f.Mirrors))]
		host = httpClient.Mirror
	}

	f.bypass(host, proxyUrl, httpClient)

	logs.Info("New client created with Mirror: %s, proxy: %s", httpClient.Mirror, httpClient.ProxyUrl)

	return httpClient
}

func (f *Fetcher) AddCookies(cookies []*http.Cookie) {
	f.cookiesMu.Lock()
	defer f.cookiesMu.Unlock()
	if f.cookies == nil {
		f.cookies = cookies
	}

	added := false
	for _, c := range cookies {
		for x, ck := range f.cookies {
			if c.Name == ck.Name {
				f.cookies[x] = c
				added = true
			}
		}
		if !added {
			f.cookies = append(f.cookies, c)
			added = false
		}
	}
}

func (f *Fetcher) HandleError(response *http.Response, err error, cmd Command) (hasErrors, brokenMirror, brokenUrl bool) {

	hasErrors, brokenMirror, brokenUrl = false, false, false

	if err == ErrDisallowed {
		return true, false, true
	}

	if response == nil {
		hasErrors = true
	} else if response.StatusCode != http.StatusOK {
		hasErrors = true
		if response.StatusCode == http.StatusServiceUnavailable &&
			strings.Contains(response.Header.Get("Server"), "cloudflare") {
			logs.Error("🔥🔥 Got Cloudflare error for %s", cmd.HttpClient().ProxyUrl)
			goto recordErr
		}
		if response.StatusCode == 549 { //Zaki Edra is still assigning the IP so resend cmd
			goto recordErr
		}
		if f.ProxyFactory != nil &&
			cmd.HttpClient().ProxyUrl != nil &&
			(response.StatusCode == 550 || //invalid IP
				response.StatusCode == 551 || //banned IP
				response.StatusCode == 552) { //auth problem
			go f.exception("🔥🔥 proxy " + cmd.HttpClient().ProxyUrl.String() + " has Auth problem!")
			return
		} else if response.StatusCode == http.StatusMovedPermanently ||
			response.StatusCode == http.StatusFound {
			redirectUrl, err := response.Location()
			if err != nil {
				go f.exception(fmt.Sprintf("Error getting redirect Url %s for Url %s Error: %v", redirectUrl, cmd.MirrorUrl(), err))
				return
			}
			logs.Notice("🔥 %d %s redirects to : %s", response.StatusCode, cmd.MirrorUrl(), redirectUrl)
			if redirectUrl.Host == cmd.MirrorUrl().Host {
				redirectUrl.Host = f.MainHost
			}
			if !f.excludeUrl(redirectUrl) {
				f.Get(redirectUrl)
			}
			//mark Url as broken to skip re-queueing & continue to add mirror error
			brokenUrl = true
			return

		} else if response.StatusCode == http.StatusNotFound {
			logs.Debug("🔥 Error with Url: %s Error: 404 Not Found", cmd.MirrorUrl())
			brokenUrl = true
			return
		} else {
			logs.Notice("🔥 %d %s", response.StatusCode, cmd.MirrorUrl())
		}
	}

	if err != nil {
		hasErrors = true
		if netError, ok := err.(net.Error); ok && netError.Timeout() {
			if f.ProxyFactory != nil && strings.Contains(err.Error(), "proxyconnect") {
				logs.Error("🔥🔥 proxy " + cmd.HttpClient().ProxyUrl.String() + " failed! Error:" + err.Error())
			} else if strings.Contains(err.Error(), "Connection refused") {
				logs.Error("🔥🔥 proxy Connection refused " + err.Error())
			} else {
				logs.Error("🔥 TIMEOUT %s", err)
			}
		}
	}

	if !hasErrors {
		return
	}

recordErr:
	f.urlErrorsMu.Lock()
	defer f.urlErrorsMu.Unlock()
	rawUrl := cmd.Url().String()
	if _, exists := f.urlErrors["urls"][rawUrl]; exists {
		f.urlErrors["urls"][rawUrl]++
	} else {
		f.urlErrors["urls"][rawUrl] = 1
	}

	if !brokenUrl {
		brokenUrl = f.urlErrors["urls"][rawUrl] > MaxUrlErrors
	}

	if f.Mirrors != nil && cmd.HttpClient() != nil {
		mirror := cmd.HttpClient().Mirror
		if _, exists := f.urlErrors["mirrors"][mirror]; exists {
			f.urlErrors["mirrors"][mirror]++
			if f.urlErrors["mirrors"][mirror] > MaxMirrorErrors {
				f.mirrorsMu.Lock()
				defer f.mirrorsMu.Unlock()
				if len(f.Mirrors) < 2 {
					f.Mirrors = nil
					logs.Error("🔥🔥 mirrors.. %s keeps failing.. Disabling it for now..", mirror)
				} else {
					for i, m := range f.Mirrors {
						if m == mirror {
							f.Mirrors = append(f.Mirrors[:i], f.Mirrors[i+1:]...)
							logs.Error("🔥🔥 mirror: %s keeps failing.. removing it", mirror)
							break
						}
					}
				}
				brokenMirror = true
			}
		} else {
			f.urlErrors["mirrors"][mirror] = 1
		}
	}

	return
}

func (f *Fetcher) passCookies(client *Client, cmd Command) {
	f.cookiesMu.RLock()
	defer f.cookiesMu.RUnlock()
	if f.cookies == nil {
		return
	}
	client.Jar.SetCookies(cmd.MirrorUrl(), f.cookies)
	logs.Debug("Set Cookies: %+v", f.cookies)
}

// Start starts the Fetcher, and returns the Queue to use to send Commands to be fetched.
func (f *Fetcher) Get(u *url.URL) *Queue {
	_, err := f.queue.SendStringGet(u)
	if err != nil {
		if err != ErrQueueClosed {
			logs.Warn("enqueue get %s - %s", u, err)
		}
	} else {
		f.snapshot.addUniqueUrl(f.uniqueId(u, "GET"))
		logs.Debug("New URL %s added", u)
	}

	return f.queue
}

func (f *Fetcher) excludeUrl(u *url.URL) bool {

	if f.StopString != "" && strings.Contains(u.String(), f.StopString) {
		go f.exception("Got stop string in the URL: " + u.String())
		return true
	}

	if u.Host != f.MainHost {
		return true
	}

	if f.snapshot.uniqueUrlExists(f.uniqueId(u, "GET")) {
		return true
	}

	requestUri := u.RequestURI()
	if f.ExcludeRegexFilter != nil && f.ExcludeRegexFilter.MatchString(requestUri) {
		return true
	}

	if f.IncludeRegexFilter != nil {
		return !f.IncludeRegexFilter.MatchString(requestUri)
	}

	return false
}

// Get the robots.txt User-Agent-specific group.
func (f *Fetcher) getRobotAgent(u *url.URL) *robotstxt.Group {
	f.robotAgentsMu.Lock()
	defer f.robotAgentsMu.Unlock()

	if agent, exists := f.robotAgents[u.Host]; exists {
		return agent
	}

	// Must send the robots.txt request.
	robotsTxtUrl := u.ResolveReference(robotsTxtParsedPath)
	// Enqueue the robots.txt request first.
	cmd := &Cmd{U: robotsTxtUrl, M: "GET", C: f.NewClient(true, false)}

	res, err, _ := f.Request(cmd, 0)
	if res == nil || err != nil {
		errString := "Error fetching robots.txt: "
		if err != nil {
			errString += err.Error()
		}
		go f.exception(errString)
		return nil
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	robData, err := robotstxt.FromResponse(res)
	if err != nil {
		go f.exception("Error parsing robots.txt: " + err.Error())
		return nil
	}
	f.robotAgents[u.Host] = robData.FindGroup(GoogleBotUserAgent)

	return f.robotAgents[u.Host]
}

func (f *Fetcher) sleeping(wakeup bool) {
	if f.LogLevel != logs.LevelDebug {
		return
	}
	f.sleepingMu.Lock()
	defer f.sleepingMu.Unlock()
	if wakeup {
		f.sleepingProcesses--
	} else {
		f.sleepingProcesses++
	}
	logs.Info("%d Goroutines sleeping", f.sleepingProcesses)
}

func (f *Fetcher) getCachedResponse(cacheId string) *http.Response {
	var page *Page
	err := f.Cache.Get(cacheId, &page)
	if err != nil && !cachita.IsErrorOk(err) {
		logs.Error("🔥 Error getting cache: %v", err)
	}
	if page == nil {
		return nil
	}

	reader := bufio.NewReader(bytes.NewReader(page.Response))
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		logs.Error("Error reading cached response: %s", err)
	}

	return response
}

func (f *Fetcher) cacheResponse(cmd Command, cacheId string, response *http.Response, forceUpdate bool) {

	if cmd.isDisableCache() {
		return
	}

	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		logs.Error("Error creating response dump: %s ", err)
		return
	}

	page := &Page{
		Id:       cacheId,
		Response: responseDump,
		RawUrl:   cmd.Url().String(),
	}

	logs.Info("new cache created for Url: %s", cmd.Url())
	go func(p *Page) {
		if f.CacheQueueSize < 2 {
			logs.Info("Caching Url %s", cmd.Url())
			err = f.Cache.Put(p.Id, p, 0)
			if err != nil {
				logs.Error("🔥 Error putting cache: %v", err)
			}
			return
		}

		f.cacheQueueMu.Lock()
		defer f.cacheQueueMu.Unlock()
		for _, v := range f.cacheQueue {
			if page.Id == v.Id {
				return
			}
		}
		f.cacheQueue = append(f.cacheQueue, p)
		if len(f.cacheQueue) > f.CacheQueueSize {
			logs.Info("Caching multiple URLs %d", len(f.cacheQueue))
			f.cachePutMulti()
			f.cacheQueue = nil
		}
	}(page)
}

func (f *Fetcher) InvalidateCache(u *url.URL, method string) {
	cacheId := f.cacheId(u, method)
	if cacheId != "" {
		f.Cache.Invalidate(cacheId)
	}
}

func (f *Fetcher) uniqueId(u *url.URL, method string) string {
	var rawUrl string
	if u.Host != f.MainHost {
		rawUrl = u.String()
	} else {
		rawUrl = u.Scheme + u.RequestURI()
	}
	return method + rawUrl
}

func (f *Fetcher) cacheId(u *url.URL, method string) string {
	if f.Cache == nil {
		return ""
	}
	rawUrl := u.RequestURI()
	if u.Host != f.MainHost {
		rawUrl = u.String()
	}
	return cachita.Id(rawUrl, method)
}

func (f *Fetcher) enqueueLinks(baseUrl *url.URL, document *goquery.Document) {
	total := 0
	// Enqueue all links as GET requests
	document.Find("a[href]").Each(func(i int, selection *goquery.Selection) {
		val, _ := selection.Attr("href")
		// Resolve address
		u, err := baseUrl.Parse(val)
		if err != nil {
			//fmt.Printf("error: resolve Url %selection - %selection\n", val, err)
			return
		}
		if u.Host == baseUrl.Host {
			u.Host = f.MainHost
		}

		if !f.excludeUrl(u) {
			u.Fragment = ""
			f.Get(u)
			total++
		}
	})
	logs.Info("Found %d total links on %s", total, baseUrl)
}

func (f *Fetcher) testMirrors() {
	if f.Mirrors == nil || f.DisableMirrorTesting {
		return
	}
	f.mirrorsMu.Lock()
	defer f.mirrorsMu.Unlock()
	httpClient := f.NewClient(true, false)
	httpClient.CheckRedirect = nil
	var mirrors []string
	for _, mirror := range f.Mirrors {
		rawUrl := "http://" + mirror + "/"
		u, err := url.Parse(rawUrl)
		if err != nil {
			go f.exception(err.Error())
			return
		}
		f.InvalidateCache(u, "GET")
		f.bypass(mirror, httpClient.ProxyUrl.String(), httpClient)
		// Do the request.
		rs, err, _ := f.Request(&Cmd{U: u, M: "GET", C: httpClient}, 0)
		if err != nil || rs.StatusCode != http.StatusOK {
			errorString := mirror + " is broken "
			if err != nil {
				errorString += " Error:" + err.Error()
			}
			if rs != nil {
				errorString += fmt.Sprintf(" Status: %d", rs.StatusCode)
				//fmt.Printf("%+v", rs)
			}
			logs.Error(errorString)
		} else {
			logs.Alert("%s looks good", mirror)
			mirrors = append(mirrors, mirror)
		}
		if rs != nil {
			rs.Body.Close()
		}
	}
	f.Mirrors = mirrors

}

func (f *Fetcher) bypass(host, proxyUrl string, httpClient *Client) {
	if f.Faloota == nil {
		return
	}
	u := "http://" + host
	cookies, err := f.Faloota.BypassOnce(u, proxyUrl, httpClient.UserAgent, f.FalootaVerify)
	if err != nil {
		logs.Error("Falouta error on url %s Error: %v", u, err)
		return
	}
	httpClient.Jar.SetCookies(h.ParseUrl(u), cookies)
	logs.Debug("added cookies to client %+v", cookies)
}
