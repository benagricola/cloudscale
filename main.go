package main

import "log"
import "os"
import "io"
import "bufio"
import "os/exec"
import "fmt"
import "sync"

func worker(wg *sync.WaitGroup, id int) {
    defer wg.Done()
    datapath := fmt.Sprintf("/data/minio-%d", id)
    address  := fmt.Sprintf("127.0.0.1:%d", id + 9000)

    log.Printf("Starting Minio worker %d with data path %s and address %s\n", id, datapath, address)
    cmd := exec.Command("/root/minio", "server", "-C", datapath, "--address", address, datapath)

    cmd.Stderr = os.Stderr
    stdin, err := cmd.StdinPipe()
    if nil != err {
    	log.Fatalf("Error obtaining stdin: %s", err.Error())
    }
    stdout, err := cmd.StdoutPipe()
    if nil != err {
    	log.Fatalf("Error obtaining stdout: %s", err.Error())
    }
    reader := bufio.NewReader(stdout)
    go func(reader io.Reader) {
    	scanner := bufio.NewScanner(reader)
    	for scanner.Scan() {
    		log.Printf("Reading from subprocess: %s", scanner.Text())
    		stdin.Write([]byte("some sample text\n"))
    	}
    }(reader)

    if err := cmd.Start(); nil != err {
    	log.Fatalf("Error starting program: %s, %s", cmd.Path, err.Error())
    }

    cmd.Wait()
}

func main() {
    var wg sync.WaitGroup

    for i := 1; i <= 2; i++ {
        wg.Add(1)
        go func(i int) {
            worker(&wg, i)
        }(i)
    }

    wg.Wait()
}
