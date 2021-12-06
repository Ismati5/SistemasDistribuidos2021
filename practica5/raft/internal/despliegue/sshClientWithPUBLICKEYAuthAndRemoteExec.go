// Implementacion de despliegue en ssh de multiples nodos
//
// Unica funcion exportada :
//		func ExecMutipleNodes(cmd string,
//							  hosts []string,
//							  results chan<- string,
//							  privKeyFile string)
//

package despliegue

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

func getHostKey(host string) ssh.PublicKey {
	// parse OpenSSH known_hosts file
	// ssh or use ssh-keyscan to get initial key
	file, err := os.Open(filepath.Join(os.Getenv("HOME"), ".ssh", "known_hosts"))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var hostKey ssh.PublicKey
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) != 3 {
			continue
		}
		if strings.Contains(fields[0], host) {
			var err error
			hostKey, _, _, _, err = ssh.ParseAuthorizedKey(scanner.Bytes())
			if err != nil {
				log.Fatalf("error parsing %q: %v", fields[2], err)
			}
			break
		}
	}

	if hostKey == nil {
		log.Fatalf("no hostkey found for %s", host)
	}

	return hostKey
}

func executeCmd(cmd, hostname string, config *ssh.ClientConfig) string {
	conn, err := ssh.Dial("tcp", hostname+":22", config)
	if err != nil {
		log.Fatalln("ERROR CONEXION SSH", err)
	}
	defer conn.Close()

	//fmt.Printf("APRES CONN %#v\n", config)

	session, err := conn.NewSession()
	if err != nil {
		log.Fatalln("ERROR SESSION", err)
	}
	defer session.Close()

	//fmt.Println("APRES SESSION")

	var stdoutBuf bytes.Buffer
	session.Stdout = &stdoutBuf
	session.Stderr = &stdoutBuf

	fmt.Println("ANTES RUN", cmd)

	session.Run(cmd)

	fmt.Println("TRAS RUN", cmd)

	return hostname + ": \n" + stdoutBuf.String()
}

func buildSSHConfig(signer ssh.Signer,
	hostKey ssh.PublicKey) *ssh.ClientConfig {

	return &ssh.ClientConfig{
		User: os.Getenv("LOGNAME"),
		Auth: []ssh.AuthMethod{
			// Use the PublicKeys method for remote authentication.
			ssh.PublicKeys(signer),
		},
		// verify host public key
		HostKeyCallback: ssh.FixedHostKey(hostKey),
		// Non-production only
		//HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		// optional tcp connect timeout
		Timeout: 5 * time.Second,
	}
}

func execOneNode(hostname string, results chan<- string,
	signer ssh.Signer, cmd string) {
	// get host public key
	// ssh_config must have option "HashKnownHosts no" !!!!
	hostKey := getHostKey(hostname)
	config := buildSSHConfig(signer, hostKey)

	fmt.Println(cmd)
	fmt.Println(hostname)

	results <- executeCmd(cmd, hostname, config)
}

// Ejecutar un mismo comando en múltiples hosts mediante ssh
func ExecMutipleNodes(cmd string,
	hosts []string,
	results chan<- string,
	privKeyFile string) {

	//results := make(chan string, 1000)

	//Read private key file for user
	pkey, err := ioutil.ReadFile(
						  filepath.Join(os.Getenv("HOME"), ".ssh", privKeyFile))
	if err != nil {
		log.Fatalf("unable to read private key: %v", err)
	}

	// Create the Signer for this private key.
	signer, err := ssh.ParsePrivateKey(pkey)
	if err != nil {
		log.Fatalf("unable to parse private key: %v", err)
	}

	for _, hostname := range hosts {
		go execOneHost(hostname, results, signer, cmd)
	}
}
