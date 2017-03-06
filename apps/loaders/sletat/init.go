package sletat

import (
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"github.com/uncleandy/tcache2/log"
	"github.com/uncleandy/tcache2/db"
)

type SletatSettings struct {
	Login 		string		`yaml:"login"`
	Password 	string		`yaml:"password"`
	Threads    	int		`yaml:"threads"`
	QueueMaxSize	int64		`yaml:"queue_max_size"`
}

func Init() {
	loadSettings()
	db.Init()
	PrepareData()
}

func loadSettings() {
	config_file := os.Getenv(EnvLoaderFileConfig)
	if config_file == "" {
		log.Error.Fatalf("Sletat loader config file name required (%s environment)", EnvLoaderFileConfig)
	}
	_, err := os.Stat(config_file)
	if os.IsNotExist(err) {
		log.Error.Fatalf("Sletat loader config file '%s' not exists.", config_file)
	}

	dat, err := ioutil.ReadFile(config_file)
	if err != nil {
		log.Error.Fatalln(err)
	}

	err = yaml.Unmarshal(dat, &sletatSettings)
	if err != nil {
		log.Error.Fatalf("error: %v", err)
	}

	request.Header.AuthInfo.Login = sletatSettings.Login
	request.Header.AuthInfo.Password = sletatSettings.Password
}
