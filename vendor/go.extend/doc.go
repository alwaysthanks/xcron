package go_extend

//extend for go1.9 build+

//beacause some package use strings.Builder
//list:
//	"github.com/grandecola/mmap"
//	"github.com/grandecola/bigqueue"
//  "github.com/robfig/cron/logger.go"
//
// the "strings.Builder" is support by go version 1.10+
// so changed the codes on up list package at places where used "strings", was replaced by "go_extend/strings"
