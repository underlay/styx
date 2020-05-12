package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	cors "github.com/rs/cors"

	styx "github.com/underlay/styx"
)

var path = os.Getenv("STYX_PATH")
var port = os.Getenv("STYX_PORT")
var prefix = os.Getenv("STYX_PREFIX")

func init() {
	if path == "" {
		log.Println("Using default path /tmp/styx")
		path = "/tmp/styx"
	}
	if port == "" {
		log.Println("Using default port 8086")
		port = "8086"
	}

	if prefix == "" {
		prefix = "http://localhost:8086"
		log.Println("Using default prefix http://localhost:8086")
	}
}

func main() {
	opt := badger.DefaultOptions(path)
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalln(err)
	}

	tags := styx.NewPrefixTagScheme(prefix)
	dictionary, err := styx.MakeIriDictionary(tags, db)
	if err != nil {
		log.Fatalln(err)
	}

	config := &styx.Config{
		TagScheme:  tags,
		Dictionary: dictionary,
		QuadStore:  styx.MakeBadgerStore(db),
	}

	store, err := styx.NewStore(config, db)

	if err != nil {
		log.Fatalln(err)
	}

	defer store.Close()

	api := &httpAPI{store: store}
	handler := cors.New(cors.Options{
		AllowCredentials: false,
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPut,
			http.MethodDelete,
		},
		AllowedHeaders: []string{"Content-Type", "Accept"},
		ExposedHeaders: []string{"Content-Type"},
		Debug:          false,
	}).Handler(api)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conns := strings.Split(r.Header.Get("Connection"), ", ")
		for _, c := range conns {
			if c == "Upgrade" && r.Header.Get("Upgrade") == "websocket" {
				handleRPC(w, r, store)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})

	log.Fatalln(http.ListenAndServe(":"+port, nil))
}
