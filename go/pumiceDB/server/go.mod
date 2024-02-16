module niova/go-pumicedb-lib/server

go 1.16

require github.com/mattn/go-pointer v0.0.1

replace niova/go-pumicedb-lib/common => ../common

require (
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.8.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
)
