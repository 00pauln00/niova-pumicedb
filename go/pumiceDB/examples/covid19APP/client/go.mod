module covidapp.com/covid_app_client

go 1.16

replace niova/go-pumicedb-lib/client => ../../../client

replace niova/go-pumicedb-lib/common => ../../../common

replace covidapplib/lib => ../lib

require (
	covidapplib/lib v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.3.0
	github.com/sirupsen/logrus v1.8.1
	niova/go-pumicedb-lib/client v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
