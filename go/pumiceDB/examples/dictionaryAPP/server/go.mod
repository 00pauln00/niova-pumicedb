module dictapp.com/pumice-dict-server

go 1.16

replace niova/go-pumicedb-lib/server => ../../../server

replace niova/go-pumicedb-lib/common => ../../../common

replace dictapplib/lib => ../lib

require (
	dictapplib/lib v0.0.0-00010101000000-000000000000
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	niova/go-pumicedb-lib/common v0.0.0-00010101000000-000000000000
	niova/go-pumicedb-lib/server v0.0.0-00010101000000-000000000000
)
