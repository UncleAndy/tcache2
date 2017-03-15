package apps_libs

import (
	"github.com/facebookgo/pidfile"
	"github.com/uncleandy/tcache2/log"
)

func PidProcess(pid_file_name string) {
	pidfile.SetPidfilePath(pid_file_name)
	old_pid, err := pidfile.Read()
	if err == nil {
		log.Error.Fatalln("Alredy runing with pid", old_pid)
	}
	err = pidfile.Write()
	if err != nil {
		log.Error.Fatalln("Can't write pid in file:", err)
	}
}