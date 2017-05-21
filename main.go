//  Written in 2014 by Matthieu Rakotojaona <matthieu.rakotojaona {on}
//  gmail.com>
//
//  To the extent possible under law, the author(s) have dedicated all
//  copyright and related and neighboring rights to this software to the
//  public domain worldwide. This software is distributed without any
//  warranty.
//
//  You should have received a copy of the CC0 Public Domain Dedication
//  along with this software. If not, see
//  <http://creativecommons.org/publicdomain/zero/1.0/>.

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
	"flag"
)

const (
	HTTP_PORT         = `15678`
	DEF_MULTICAST_ADDRESS = `224.3.45.67:15679`
	TTL               = 1 * time.Hour
	MULTICAST_DELAY   = 10 * time.Minute

	// Note that we only provide packages, not dbs
	PKG_CACHE_DIR = `/var/cache/pacman/pkg`
)

var (
	peers    = newPeerMap()
	seenTags = newTagMap()
)

type peerMap struct {
	sync.Mutex
	peers  map[string]struct{}
	expire chan string
}

func newPeerMap() peerMap {
	p := peerMap{
		peers:  make(map[string]struct{}),
		expire: make(chan string),
	}
	go p.expireLoop()

	return p
}

func (p peerMap) expireLoop() {
	for {
		select {
		case peer := <-p.expire:
			p.Lock()
			delete(p.peers, peer)
			p.Unlock()
		}
	}
}

func (p peerMap) Add(peer string) {
	p.Lock()
	p.peers[peer] = struct{}{}
	time.AfterFunc(TTL, func() { p.expire <- peer })
	p.Unlock()
}

func (p peerMap) GetRandomOrder() []string {
	p.Lock()

	peers := make([]string, len(p.peers))
	for peer := range p.peers {
		max := big.NewInt(int64(len(peers)))
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			log.Printf("Couldn't get random int: %s\n", err)
			continue
		}
		peers[idx.Int64()] = peer
	}

	p.Unlock()

	return peers
}

type TagMap struct {
	sync.Mutex
	tags   map[string]struct{}
	expire chan string
}

func newTagMap() TagMap {
	t := TagMap{
		tags:   make(map[string]struct{}),
		expire: make(chan string),
	}

	go func() {
		for {
			select {
			case tag := <-t.expire:
				delete(t.tags, tag)
			}
		}
	}()

	return t
}

func (t TagMap) Mark(tag string) {
	t.Lock()
	t.tags[tag] = struct{}{}
	time.AfterFunc(TTL, func() { t.expire <- tag })
	t.Unlock()
}

func (t TagMap) IsNew(tag string) bool {
	t.Lock()
	_, ok := t.tags[tag]
	t.Unlock()

	return !ok
}

func main() {
	destAddrsPtr := flag.String("addrs", "", "additional, static peer addresses")
	mcAddrPtr := flag.String("multicast", DEF_MULTICAST_ADDRESS, "multicast address")
	flag.Parse()
	
	go serveMulticast(*mcAddrPtr, *destAddrsPtr)
	go serveHttp()

	go func() {
		for _ = range time.Tick(10 * time.Minute) {
			peers.Lock()
			peerSlice := make([]string, 0, len(peers.peers))
			for peer, _ := range peers.peers {
				peerSlice = append(peerSlice, peer)
			}
			log.Printf("Got %d peers: %s\n", len(peerSlice), strings.Join(peerSlice, ","))
			peers.Unlock()
		}
	}()

	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, os.Kill)

	select {
	case <-c:
		return
	}
}

func serveHttp() {
	http.Handle("/", http.HandlerFunc(handle))

	log.Println("Serving from", HTTP_PORT)
	err := http.ListenAndServe(":"+HTTP_PORT, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	addr, err := net.ResolveTCPAddr("tcp", r.RemoteAddr)
	if err != nil {
		log.Printf("Error serving %s: %s\n", r.RemoteAddr, err)
		return
	}

	if addr.IP.IsLoopback() {
		handleLocal(w, r)
	} else {
		handleRemote(w, r)
	}
}

func handleLocal(w http.ResponseWriter, r *http.Request) {
	for _, peer := range peers.GetRandomOrder() {
		newUrl := *r.URL
		newUrl.Host = peer
		newUrl.Scheme = "http"

		resp, err := http.Head(newUrl.String())
		if err == nil {
			if r.Method == "HEAD" {
				w.WriteHeader(resp.StatusCode)
				return
			} else if r.Method == "GET" && resp.StatusCode == http.StatusOK {
				http.Redirect(w, r, newUrl.String(), http.StatusFound)
				return
			}
		}
	}

	w.WriteHeader(http.StatusNotFound)
}

func handleRemote(w http.ResponseWriter, r *http.Request) {
	fpath := path.Join(PKG_CACHE_DIR, path.Base(r.URL.Path))
	_, err := os.Stat(fpath)

	if err == nil {
		if r.Method == "HEAD" {
			w.WriteHeader(http.StatusOK)
		} else if r.Method == "GET" {
			http.ServeFile(w, r, fpath)
		}
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

type Announce struct {
	Port string `json:"port"`
	Tag  string `json:"tag"`
}

type multicaster struct {
	conn *net.UDPConn
	addrs []*net.UDPAddr
}

func serveMulticast(multicastAddrOption string, destAddrOption string) {
	destHosts := strings.Split(destAddrOption, ",")
	destIPList := make([]*net.UDPAddr, len(destHosts)+1)
	for i := 0; i < len(destHosts); i++ {
		a, err := net.ResolveUDPAddr("udp4", destHosts[i])
		if err != nil {
			return
		} else {
			destIPList[i] = a
		}
	}
	
	mcAddr, err := net.ResolveUDPAddr("udp4", multicastAddrOption)
	if err != nil {
		return
	}
	conn, err := net.ListenMulticastUDP("udp4", nil, mcAddr)
	if err != nil {
		return
	}

	mc := multicaster{conn: conn, addrs: destIPList}
	mc.run()
}

func (mc multicaster) run() {
	go mc.listenLoop()

	mc.sendAnnounce()
	for {
		<-time.After(MULTICAST_DELAY)
		mc.sendAnnounce()
	}
}

func (mc multicaster) sendAnnounce() {
	tagRaw := make([]byte, 8)
	_, err := rand.Read(tagRaw)
	if err != nil {
		log.Printf("Couldn't create tag: %s\n", err)
		return
	}

	tag := hex.EncodeToString(tagRaw)
	mc.sendAnnounceWithTag(tag)
}

func (mc multicaster) sendAnnounceWithTag(tag string) {
	msg := Announce{Port: HTTP_PORT, Tag: tag}
	raw, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Couldn't serialize announce: %s\n", err)
		return
	}
	for _, addr := range mc.addrs {
		mc.conn.WriteToUDP(raw, addr)
	}
	seenTags.Mark(msg.Tag)
}

func (mc multicaster) listenLoop() {
	for {
		packet := make([]byte, 256)
		_, from, err := mc.conn.ReadFromUDP(packet)
		if err != nil {
			log.Printf("Error reading from %s: %s\n", from, err)
			continue
		}

		var msg Announce
		err = json.NewDecoder(bytes.NewReader(packet)).Decode(&msg)
		if err != nil {
			log.Printf("Couldn't unserialize announce [%s]: %s\n", packet, err)
			continue
		}

		if seenTags.IsNew(msg.Tag) {
			mc.sendAnnounceWithTag(msg.Tag)
		}
		peer := net.JoinHostPort(from.IP.String(), msg.Port)
		log.Printf("New peer: %s\n", peer)
		peers.Add(peer)
	}
}
