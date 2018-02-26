package main

import (
    "log"
    "os"
    "gopkg.in/yaml.v2"
    "io"
    "io/ioutil"
    "bufio"
    "os/exec"
    "fmt"
    "math"
    "strings"
    "regexp"
    "time"
    "net/url"
    "net/http"
    "net/http/httputil"
    
    "github.com/jessevdk/go-flags"
)


type Options struct {
    Config string `short:"c" long:"config" description:"Config file to load settings from" required:"true"`
}

type ConfigProgram struct {
    Binary string   `yaml:"binary"`
    Args   []string `yaml:"args"`
    Env    []string `yaml:"env"`
}

type Config struct {
    Program   ConfigProgram                     `yaml:"program"` 
    Id_start  int                               `yaml:"id_start"`
    Header    string                            `yaml:"header"`
    Regex     string                            `yaml:"regex"`
    Max_procs int                               `yaml:"max_procs"`
    Timeout   int                               `yaml:"timeout"`
    Bind      string                            `yaml:"bind"`
    Data      map[string]map[string]interface{} `yaml:"data"`
}

var options Options

func Tprintf(format string, params map[string]interface{}) string {
    for key, val := range params {
        format = strings.Replace(format, "%{"+key+"}s", fmt.Sprintf("%s", val), -1)
        format = strings.Replace(format, "%{"+key+"}d", fmt.Sprintf("%d", val), -1)
    }
    return fmt.Sprintf(format)
}

func worker(config *Config, data map[string]interface{}) *exec.Cmd {

    var cmdline []string
    var env []string

    for _, v := range config.Program.Args {
        cmdline = append(cmdline, Tprintf(v, data))
    }

    for _, v := range config.Program.Env {
        env = append(env, Tprintf(v, data))
    }

    log.Printf("Starting worker process with command line %+v\n", cmdline)
    cmd := exec.Command(config.Program.Binary, cmdline...)

    // Set command environment
    cmd.Env = env

    cmd.Stderr = os.Stderr

    _, err := cmd.StdinPipe()
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
            log.Printf("[ID %d]: %s", data["id"], scanner.Text())
        }
    }(reader)

    if err := cmd.Start(); nil != err {
        log.Fatalf("Error starting program: %s, %s", cmd.Path, err.Error())
    }

    return cmd
}

func loadConfig(Conf_file string) *Config {
    config := &Config{
        Program: ConfigProgram {
            Binary: "minio",
        },
        Max_procs: 100, 
        Id_start: 15000,
        Timeout: 300, 
        Bind: "localhost:4901",
    }

    yamlFile, err := ioutil.ReadFile(Conf_file)
    
    if nil != err {
        log.Fatalf("Error reading config file %s: %s", Conf_file, err.Error())
    }

    if err = yaml.Unmarshal(yamlFile, &config); nil != err {
        log.Fatalf("Error reading config file %s: %s", Conf_file, err.Error())
    }

    max_id := int(math.Pow(2, 16)) - config.Max_procs

    if config.Id_start > max_id {
        log.Fatalf("id_start may not be more than %d, in case ID is used as a port number.", max_id)
    }
    return config
}

func main() {

    if _, err := flags.Parse(&options); nil != err {
        log.Fatalf("Error parsing commandline options: %s", err.Error())
    }

    config := loadConfig(options.Config)


    log.Printf("Binding on %s for new HTTP connections...", config.Bind)
    log.Printf("Child Processes will start with IDs %d - %d...", config.Id_start, config.Id_start + config.Max_procs-1)

    // Check that regexp compiles
    header_regex, err := regexp.Compile(config.Regex)

    if nil != err {
        log.Fatalf("Error compiling header regex %s: %s", config.Regex, err.Error())
    }

    proxies      := make(map[int] *httputil.ReverseProxy)
    entry_status := make(map[int] string)
    entry_cmd    := make(map[int] *exec.Cmd)
    entry_idle   := make(map[int] time.Time)

    entry_ctr := config.Id_start

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        header := r.Header.Get(config.Header)
        if header == "" {
            log.Printf("Header %s not found", config.Header)
            http.Error(w, "Access Denied", 401)
            return
        }

        header_matched := header_regex.FindStringSubmatch(header)
        if header_matched == nil || "" == header_matched[1] {
            log.Printf("Header regex %s not matched on header %s with value %s", config.Regex, config.Header, header)
            http.Error(w, "Access Denied", 401)
            return
        }

        key := header_matched[1]

        entry, ok := config.Data[key]
        if !ok {
            log.Printf("Unable to find data entry for value %s", header)
            http.Error(w, "Access Denied", 401)
            return
        }

        // Get the ID uniquely assigned to this entry or assign one
        entry_id, ok := entry["id"].(int)
        if !ok {
            log.Printf("Entry %s does not have an ID asigned - allocating %d", key, entry_ctr)
            entry_id = entry_ctr
            entry["id"] = entry_id
            entry["key"] = key
            entry_ctr ++
        }

        // Now we need to work out if we need to start a process
        status, ok := entry_status[entry_id]
        if status == "stopped" || !ok {
            if len(entry_cmd) >= config.Max_procs {
                log.Printf("Entry %s has no running process but max procs of %d reached!", key, config.Max_procs)
                http.Error(w, "Access Denied", 401)
                return
            }

            log.Printf("Entry %s has no running process, starting...", key)

            // Worker loop, runs until worker dies and then cleans itself up
            go func(entry map[string]interface{}, entry_id int) {
                cmd := worker(config, entry)
                entry_cmd[entry_id] = cmd
                cmd.Wait()
                log.Printf("Worker process for entry %s died or was killed...", entry["key"])
                entry_status[entry_id] = "stopped"
                delete(entry_idle, entry_id)
                delete(entry_cmd, entry_id)
            }(entry, entry_id)

            // Set this process as starting
            status = "starting"
            entry_status[entry_id] = status
        }


        if status == "starting" {
            log.Printf("Waiting for worker process with entry %s to start listening on %d", key, entry_id)

            addr := fmt.Sprintf("http://127.0.0.1:%d", entry_id)

            // Timeout HTTP requests after 1 second
            http_client := &http.Client{
                Timeout: 1 * time.Second,
            }

            for {
                // If process dies before starting, exit loop
                if entry_status[entry_id] != "starting" {
                    log.Printf("Entry %s process died on startup!", key)
                    http.Error(w, "Bad Gateway", 502)
                    return
                }

                // Make HTTP request to service. If it returns >= 500, assume not started yet
                resp, err := http_client.Get(addr)
                if err == nil {
                    defer resp.Body.Close()
                    if resp.StatusCode < 500 {
                        log.Printf("Worker process for entry %s is listening on %d", key, entry_id)
                        status = "started"
                        entry_status[entry_id] = status
                        break
                    } else {
                        log.Printf("Worker process startup check for entry %s received error status: %+v", key, resp.StatusCode)
                    }

                } else {
                    log.Printf("Worker process startup check for entry %s received error: %+v", key, err)
                }
                // Sleep here to avoid request spam 
                time.Sleep(500 * time.Millisecond)
            }
        }

        proxy, ok := proxies[entry_id]

        if !ok {
            url := url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", entry_id), Path: "/"}
            proxy = httputil.NewSingleHostReverseProxy(&url)
            proxies[entry_id] = proxy
        }
        r.Header.Set("Host", r.Host)

        entry_idle[entry_id] = time.Now()
        proxy.ServeHTTP(w, r)
    })

    log.Printf("Cloudscale running...")

    // Periodically reap inactive processes
    go func() {
        timeout := time.Duration(config.Timeout) * time.Second
        for {
            <-time.After(5 * time.Second)
            now := time.Now()
            for entry_id, last_activity := range entry_idle {
                idle_time := now.Sub(last_activity)
                if idle_time > timeout {
                    cmd := entry_cmd[entry_id]
                    log.Printf("Reaping process for entry ID %d with idle timer %+v", entry_id, idle_time)
                    cmd.Process.Kill()
                }
            }
        }
    }()
    log.Fatal(http.ListenAndServe(config.Bind, nil))
}
