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
	paclanId = generateRandomTag()
)

type peerMap struct {
	sync.Mutex
	peers  map[string]peerInfo
	expire chan string
}
type peerInfo struct {
	httpOrigin string
	expire time.Time
	renew time.Time
	id string
}
func newPeerMap() peerMap {
	p := peerMap{
		peers:  make(map[string]peerInfo),
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
			if time.Now().Before(p.peers[peer].expire) {
				delete(p.peers, peer)
			}
			p.Unlock()
		}
	}
}

func (p peerMap) Add(peer string, httpServer string) {
	p.Lock()
	p.peers[peer] = peerInfo {
		expire: time.Now().Add(TTL),
		renew: time.Now().Add(MULTICAST_DELAY),
		httpOrigin: httpServer,
	}
	time.AfterFunc(TTL, func() { p.expire <- peer })
	p.Unlock()
}

func (p peerMap) Has(peerIp string) bool {
	p.Lock()
	_, has := p.peers[peerIp]
	p.Unlock()
	return has
}
func (p peerMap) ShouldRenew(peerIp string) bool {
	p.Lock()
	peer, has := p.peers[peerIp]
	p.Unlock()
	return !has || time.Now().Before(peer.renew) 
}

func (p peerMap) GetHttpHostsInRandomOrder() []string {
	p.Lock()

	peers := make([]string, len(p.peers))
	for _, peer := range p.peers {
		max := big.NewInt(int64(len(peers)))
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			log.Printf("Couldn't get random int: %s\n", err)
			continue
		}
		peers[idx.Int64()] = peer.httpOrigin
	}

	p.Unlock()

	return peers
}
func (p peerMap) GetPeerList() []string {
	p.Lock()

	peers := make([]string, len(p.peers))
	i := 0
	for ip, peer := range p.peers {
		peers[i] = peer.id + "@" + ip
		i += 1
	}

	p.Unlock()

	return peers
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

func generateRandomTag() string {
	tagRaw := make([]byte, 8)
	_, err := rand.Read(tagRaw)
	if err != nil {
		log.Printf("Couldn't create tag: %s\n", err)
		return ""
	}

	return hex.EncodeToString(tagRaw)
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
	for _, peer := range peers.GetHttpHostsInRandomOrder() {
		newUrl := *r.URL
		newUrl.Host = peer
		newUrl.Scheme = "http"

		resp, err := http.Head(newUrl.String())
		if err == nil {
			if r.Method == "HEAD" {
				log.Printf("Handling local HEAD request, status=%d, url=%s\n", resp.StatusCode, newUrl.String())
				w.WriteHeader(resp.StatusCode)
				return
			} else if r.Method == "GET" && resp.StatusCode == http.StatusOK {
				log.Printf("Handling local GET request, status=%d, url=%s\n", resp.StatusCode, newUrl.String())
				http.Redirect(w, r, newUrl.String(), http.StatusFound)
				return
			}
		}
	}

	log.Printf("No match for local request, url=%s\n", r.URL.String())
	w.WriteHeader(http.StatusNotFound)
}

func handleRemote(w http.ResponseWriter, r *http.Request) {
	fpath := path.Join(PKG_CACHE_DIR, path.Base(r.URL.Path))
	_, err := os.Stat(fpath)
	r.Header.Add("X-Paclan-ID", paclanId)
	
	if err == nil {
		if r.Method == "HEAD" {
			log.Printf("[%s] Remote HEAD request success, path=%s\n", r.RemoteAddr, r.URL.Path)
			w.WriteHeader(http.StatusOK)
		} else if r.Method == "GET" {
			log.Printf("[%s] Serving file, path=%s\n", r.RemoteAddr, r.URL.Path)
			http.ServeFile(w, r, fpath)
		}
		return
	}

	log.Printf("[%s] Not found, path=%s\n", r.RemoteAddr, r.URL.Path)
	w.WriteHeader(http.StatusNotFound)
}

type Announce struct {
	Type  string `json:"TYPE"`
	HttpPort string `json:"PORT"`
	Id string `json:"ID"`
	Nonce string `json:"NONCE"`
	Peers []string `json:"PEERS"`
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
			destIPList[i+1] = a
		}
	}
	
	mcAddr, err := net.ResolveUDPAddr("udp4", multicastAddrOption)
	if err != nil {
		return
	}
	destIPList[0] = mcAddr
	
	conn, err := net.ListenMulticastUDP("udp4", nil, mcAddr)
	if err != nil {
		return
	}

	mc := multicaster{conn: conn, addrs: destIPList}
	mc.run()
}

func (mc multicaster) run() {
	go mc.listenLoop()

	mc.sendAnnounce("PING", "", []string{})
	for {
		<-time.After(MULTICAST_DELAY)
		mypeers := peers.GetPeerList()
		mc.sendAnnounce("PING", "", mypeers)
	}
}


func (mc multicaster) sendAnnounce(typ string, nonce string, peerlist []string) {
	raw := buildAnnounce(typ, nonce, peerlist)
	log.Print(raw)
	for _, addr := range mc.addrs {
		mc.conn.WriteToUDP(raw, addr)
	}
}
func buildAnnounce(typ string, nonce string, peerlist []string)  []byte{
	msg := Announce{HttpPort: HTTP_PORT, Type: typ, Nonce: "", Id: paclanId, Peers: peerlist}
	raw, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Couldn't serialize announce: %s\n", err)
		return nil
	}
	return raw
}

func onPeerFound(peerIp string, peerHttp string, peerPaclanId string) {
	if peerPaclanId == paclanId {
		// don't talk to myself...
		return
	}
	if peers.Has(peerIp) {
		peers.Add(peerIp, peerHttp)
		return
	}
	resp, err := http.Head("http://" + peerHttp)
	if err == nil {
		if resp.Header.Get("X-Paclan-ID") == peerPaclanId {
			log.Printf("New peer verified with id=%s, url=http://%s\n", peerPaclanId, peerHttp)
			peers.Add(peerIp, peerHttp)
		} else {
			log.Printf("Peer verification failed, udp_id=%s, http_id=%s, url=http://%s\n",
				peerPaclanId, resp.Header.Get("X-Paclan-ID"), peerHttp)
		}
	}
}

func (mc multicaster) discoverPeers(discopeers []string) {
	raw_msg := buildAnnounce("PING", "", []string{})
	if raw_msg == nil { return }
	
	for _, peer := range discopeers {
		log.Printf("discoverPeer: %s\n", peer)
		data := strings.SplitN(peer, "@", 2)
		if data[0] != paclanId && peers.ShouldRenew(data[1]){
			discoverTarget, err := net.ResolveUDPAddr("udp4", data[1] + ":15679")
			if err != nil {
				return
			}
			mc.conn.WriteToUDP(raw_msg, discoverTarget)
		}
	}
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

		peerIp := from.IP.String()
		peerHttp := net.JoinHostPort(peerIp, msg.HttpPort)
		log.Printf("Received message type=%s, from peer=%s\n", msg.Type, peerIp)
		switch msg.Type {
		case "PING":
			mypeers := peers.GetPeerList()
			mc.sendAnnounce("PONG", "", mypeers)
			onPeerFound(peerIp, peerHttp, msg.Id)
			mc.discoverPeers(msg.Peers)
		case "PONG":
			//mc.sendAnnounceWithTag("ACK", "", mypeers)
			onPeerFound(peerIp, peerHttp, msg.Id)
			mc.discoverPeers(msg.Peers)
		}
	}
}
