DIR=/usr/local/niova
CGO_LDFLAGS=-L/${DIR}/lib/
CGO_CFLAGS=-I/${DIR}/include/
LD_LIBRARY_PATH=/${DIR}/niova-core/lib/

export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

niovakv_all: compile niovakv covidapp foodpalaceapp leaseApp install

compile:
	echo "Compiling niovakv"
	mkdir -p libexec

niovakv:
	go build -o libexec/NKV_pmdbServer apps/niovaKV/pmdbServer/pmdbServer.go
	go build -o libexec/NKV_proxy apps/niovaKV/proxy/proxy.go
	go build -o libexec/nkvc apps/niovaKV/nkvc/nkvc.go

covidapp:
	go build -o libexec/covid_app_server apps/covid19APP/server/covid_app_server.go
	go build -o libexec/covid_app_client apps/covid19APP/client/covid_app_client.go

foodpalaceapp:
	go build -o libexec/foodpalaceappserver apps/foodpalaceAPP/server/foodpalaceappServer.go
	go build -o libexec/foodpalaceappclient apps/foodpalaceAPP/client/foodpalaceappClient.go

leaseApp:
	go build -o libexec/LeaseApp_pmdbServer apps/leaseApp/pmdbServer/pmdbServer.go
	go build -o libexec/leaseClient apps/leaseApp/client/leaseClient.go


install:
	cp libexec/NKV_pmdbServer ${DIR}/libexec/niova/
	cp libexec/NKV_proxy ${DIR}/libexec/niova/
	cp libexec/nkvc ${DIR}/libexec/niova/
	cp libexec/covid_app_server ${DIR}/libexec/niova/
	cp libexec/covid_app_client ${DIR}/libexec/niova/
	cp libexec/foodpalaceappserver ${DIR}/libexec/niova/
	cp libexec/foodpalaceappclient ${DIR}/libexec/niova/
	cp libexec/LeaseApp_pmdbServer ${DIR}/libexec/niova/
	cp libexec/leaseClient ${DIR}/libexec/niova/

clean:
	rm -rf libexec

