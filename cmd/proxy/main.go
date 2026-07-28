// Thank you @Cbro for the initial code snippets
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"pgcr-processing-service/internal/utils"

	"golang.org/x/time/rate"
)

var (
	ipv6n      = flag.Int("v6_n", 16, "Number of sequential Ipv6 addresses")
	port       = flag.Int("port", 8081, "Port to listen on")
	printAddrs = flag.Bool("print_addrs", false, "Print Ipv6 addresses")
	verbose    = flag.Bool("verbose", false, "Print logs")
)

var (
	rateIntervalSeconds = 10
	rateInterval        = time.Second * time.Duration(rateIntervalSeconds)
)

type transport struct {
	nW      atomic.Int64
	nS      atomic.Int64
	rt      []http.RoundTripper
	statsRl []*rate.Limiter
	wwwRl   []*rate.Limiter
}

var (
	proxyTransport        = &transport{}
	statsDomain    string = "stats.bungie.net"
	baseDomain     string = "www.bungie.net"
	statsPath      string = "Destiny2/Stats/PostGameCarnageReport"
)

func main() {
	flag.Parse()

	addressPath := os.Getenv("INITIAL_ADDR")
	if addressPath == "" {
		log.Fatal("INITIAL_ADDR env variable must be passed")
	}

	address, err := utils.ReadSecret(addressPath)
	if err != nil {
		log.Fatalf("Error parsing IPv6 address from docker secret: %v", err)
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		log.Fatalf("Unable to get network interfaces for host: %v", err)
	}

	_, targetSubnet, err := net.ParseCIDR("2604:a880:4:1d0::/64")
	if err != nil {
		log.Fatalf("Unable to parse CIDR block: %v", err)
	}

	var ipv6interface string

Outer:
	for _, i := range interfaces {
		addresses, err := i.Addrs()
		if err != nil {
			log.Fatalf("Error reading addresses for interface %s: %v", i.Name, err)
		}
		for _, a := range addresses {
			ip, ok := a.(*net.IPNet)
			if !ok {
				log.Fatalf("Cannot assert type *net.IPNet from %T", ip)
			}

			if targetSubnet.Contains(ip.IP) {
				ipv6interface = i.Name
				break Outer
			}
		}
	}
	addr := netip.MustParseAddr(address)

	for range *ipv6n {
		cmd := exec.Command("ip", "-6", "addr", "add", fmt.Sprintf("%s/64", addr.String()), "dev", ipv6interface)

		if output, err := cmd.CombinedOutput(); err != nil {
			if *verbose {
				log.Printf("Failed to add IP %s: %v | Output: %s", addr.String(), err, string(output))
			}
		} else if *verbose {
			log.Printf("Successfully plumbed %s onto %s", addr.String(), ipv6interface)
		}

		d := &net.Dialer{
			LocalAddr: &net.TCPAddr{
				IP: net.IP(addr.AsSlice()),
			},
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		rt := http.DefaultTransport.(*http.Transport).Clone()
		rt.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				log.Fatalf("Something happened while building transport: %v", err)
			}
			return conn, err
		}

		if *printAddrs {
			fmt.Printf("ip -6 addr add %s/64 dev %s\n", addr.String(), ipv6interface)
		}

		proxyTransport.statsRl = append(proxyTransport.statsRl, rate.NewLimiter(rate.Every(time.Second/40), 90))
		proxyTransport.wwwRl = append(proxyTransport.wwwRl, rate.NewLimiter(rate.Every(time.Second/40), 90))
		proxyTransport.rt = append(proxyTransport.rt, rt)
		addr = addr.Next()
	}

	rp := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			if strings.Contains(r.URL.Path, statsPath) {
				r.URL.Host = statsDomain
			} else {
				r.URL.Host = baseDomain
			}
			r.URL.Scheme = "https"
			r.Header.Set("User-Agent", "rivenbot")
			r.Header.Del("x-forwarded-for")
		},
		Transport: proxyTransport,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "Ok")
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-betteruptime-probe") != "" {
			io.WriteString(w, "Ok")
			return
		}
		rp.ServeHTTP(w, r)
	})

	log.Printf("Ready on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), mux))
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	var rl *rate.Limiter
	var n int64

	if strings.Contains(r.URL.Path, statsPath) {
		n = t.nS.Add(1)
		r.Host = statsDomain
		rl = t.statsRl[n%int64(len(t.statsRl))]
	} else {
		n = t.nW.Add(1)
		r.Host = baseDomain
		rl = t.wwwRl[n%int64(len(t.wwwRl))]
	}

	if *verbose {
		log.Printf("Sending request: %s\n", r.URL.String())
		log.Printf("Request headers: %s\n", r.Header)
	}
	rt := t.rt[n%int64(len(t.rt))]
	rl.Wait(r.Context())
	return rt.RoundTrip(r)
}
