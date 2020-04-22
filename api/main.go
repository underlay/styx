package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	cors "github.com/rs/cors"

	styx "github.com/underlay/styx"
)

var path = os.Getenv("STYX_PATH")
var port = os.Getenv("STYX_PORT")
var prefix = os.Getenv("STYX_PREFIX")

func main() {
	tags := styx.NewPrefixTagScheme(prefix)
	store, err := styx.NewStore(&styx.Config{
		Path:      path,
		TagScheme: tags,
	})

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

	if port == "" {
		port = "8086"
	}

	log.Fatalln(http.ListenAndServe(":"+port, nil))
}
