SRC_DIR=$(pwd)
DST_DIR=$(pwd)
protoc -I=$SRC_DIR --go_out=$DST_DIR $SRC_DIR/styx.proto