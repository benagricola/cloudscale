package main

import "fmt"
import "io/ioutil"
import "os/exec"
import "github.com/minio/minio"

func main() {
 go minio.newApp(args[0]).Run(args)
}
