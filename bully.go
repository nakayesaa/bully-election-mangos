// Bully election: eight independent processes communicating over mangos TCP.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

const nodeCount = 8
const timeout = time.Second

type message struct {
	Kind string `json:"kind"`
	From int    `json:"from"`
}

func address(id int) string { return fmt.Sprintf("tcp://127.0.0.1:%d", 5600+id) }

// Each request gets its own socket: concurrent elections cannot mix replies.
func request(id int, m message) (message, error) {
	s, err := req.NewSocket()
	if err != nil {
		return message{}, err
	}
	defer s.Close()
	for k, v := range map[string]interface{}{
		mangos.OptionSendDeadline: timeout,
		mangos.OptionRecvDeadline: timeout,
		mangos.OptionRetryTime:    time.Duration(0),
	} {
		if err = s.SetOption(k, v); err != nil {
			return message{}, err
		}
	}
	if err = s.DialOptions(address(id), map[string]interface{}{mangos.OptionDialAsynch: true}); err != nil {
		return message{}, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return message{}, err
	}
	if err = s.Send(b); err != nil {
		return message{}, err
	}
	b, err = s.Recv()
	if err != nil {
		return message{}, err
	}
	var reply message
	err = json.Unmarshal(b, &reply)
	if err == nil && reply.From != id {
		err = fmt.Errorf("unexpected reply sender %d", reply.From)
	}
	return reply, err
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		log.Fatal("usage: ./bully ID [live|live_init|dead]")
	}
	id, err := strconv.Atoi(os.Args[1])
	if err != nil || id < 0 || id >= nodeCount {
		log.Fatal("ID must be 0..7")
	}
	mode := "live"
	if id == 4 {
		mode = "live_init"
	}
	if id == 7 {
		mode = "dead"
	}
	if len(os.Args) == 3 {
		mode = os.Args[2]
	}
	if mode != "live" && mode != "live_init" && mode != "dead" {
		log.Fatal("invalid mode")
	}
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("[node %d] ", id))
	if mode == "dead" {
		log.Print("DEAD: tidak membuka socket")
		return
	}
	s, err := rep.NewSocket()
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	if err = s.Listen(address(id)); err != nil {
		log.Fatal(err)
	}
	inbox := make(chan message, 32)
	failures := make(chan error, 1)
	go func() {
		for {
			b, e := s.Recv()
			if e != nil {
				failures <- e
				return
			}
			var m message
			if json.Unmarshal(b, &m) != nil || m.From < 0 || m.From >= nodeCount {
				b = []byte(`{"kind":"ERROR","from":` + strconv.Itoa(id) + `}`)
			} else {
				kind := "ACK"
				if m.Kind == "ELECTION" && m.From < id {
					kind = "OK"
				}
				b, _ = json.Marshal(message{kind, id})
			}
			if e = s.Send(b); e != nil {
				failures <- e
				return
			}
			if m.From >= 0 && m.From < nodeCount && (m.Kind == "ELECTION" || m.Kind == "COORDINATOR") {
				inbox <- m
			}
		}
	}()
	log.Printf("LIVE pada %s; coordinator awal=7", address(id))
	var trigger <-chan time.Time
	if mode == "live_init" {
		trigger = time.After(2 * time.Second)
	}
	results := make(chan bool, 1)
	started := false
	leader := 7
	start := func() {
		if started {
			return
		}
		started = true
		log.Print("mulai ELECTION")
		go func() {
			replies := make(chan bool, nodeCount)
			for peer := id + 1; peer < nodeCount; peer++ {
				log.Printf("SEND ELECTION -> %d", peer)
				go func(p int) {
					r, e := request(p, message{"ELECTION", id})
					ok := e == nil && r.Kind == "OK"
					if ok {
						log.Printf("RECV OK <- %d", p)
					} else {
						log.Printf("tidak ada OK dari %d (timeout/error)", p)
					}
					replies <- ok
				}(peer)
			}
			anyOK := false
			for peer := id + 1; peer < nodeCount; peer++ {
				if <-replies {
					anyOK = true
				}
			}
			results <- anyOK
		}()
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	for {
		select {
		case <-trigger:
			trigger = nil
			log.Print("simulasi: node 4 mengetahui coordinator 7 mati")
			start()
		case m := <-inbox:
			switch m.Kind {
			case "ELECTION":
				if m.From < id {
					log.Printf("RECV ELECTION <- %d; SEND OK -> %d", m.From, m.From)
					start()
				}
			case "COORDINATOR":
				if m.From > id {
					leader = m.From
					log.Printf("RECV COORDINATOR <- %d; coordinator=%d", m.From, leader)
				}
			}
		case anyOK := <-results:
			if leader != 7 {
				continue
			}
			if anyOK {
				log.Print("diam: menunggu COORDINATOR")
				continue
			}
			leader = id
			log.Printf("MENJADI COORDINATOR: %d", id)
			for peer := 0; peer < id; peer++ {
				log.Printf("SEND COORDINATOR -> %d", peer)
				go func(p int) {
					r, e := request(p, message{"COORDINATOR", id})
					if e != nil || r.Kind != "ACK" {
						log.Printf("pengumuman ke %d gagal: %v", p, e)
					}
				}(peer)
			}
		case e := <-failures:
			log.Fatal(e)
		case <-stop:
			return
		}
	}
}
