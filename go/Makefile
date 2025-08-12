DIR=/usr/local/niova
ASAN_LIB_PATH=/lib/x86_64-linux-gnu
CGO_LDFLAGS=-L/${DIR}/lib/ -L${ASAN_LIB_PATH} -lasan
CGO_CFLAGS=-I/${DIR}/include/ 
LD_LIBRARY_PATH=${ASAN_LIB_PATH}:/${DIR}/niova-core/lib/

export CGO_ENABLED=1
export CGO_LDFLAGS
export CGO_CFLAGS
export LD_LIBRARY_PATH
export PATH

ASAN_LDFLAGS=-ldflags="-extldflags '-L${ASAN_LIB_PATH} -lasan'"

niovakv_all: compile niovakv covidapp foodpalaceapp leaseApp install

compile:
	echo "Compiling niovakv"
	mkdir -p libexec
	go mod tidy

niovakv:
	go build ${ASAN_LDFLAGS} -o libexec/NKV_pmdbServer apps/niovaKV/pmdbServer/pmdbServer.go
	go build ${ASAN_LDFLAGS} -o libexec/NKV_proxy apps/niovaKV/proxy/proxy.go
	go build ${ASAN_LDFLAGS} -o libexec/nkvc apps/niovaKV/nkvc/nkvc.go

covidapp:
	go build ${ASAN_LDFLAGS} -o libexec/covid_app_server apps/covid19APP/server/covid_app_server.go
	go build ${ASAN_LDFLAGS} -o libexec/covid_app_client apps/covid19APP/client/covid_app_client.go
	cp apps/covid19APP/client/vaccinations.csv libexec/

foodpalaceapp:
	go build ${ASAN_LDFLAGS} -o libexec/foodpalaceappserver apps/foodpalaceAPP/server/foodpalaceappServer.go
	go build ${ASAN_LDFLAGS} -o libexec/foodpalaceappclient apps/foodpalaceAPP/client/foodpalaceappClient.go

leaseApp:
	go build ${ASAN_LDFLAGS} -o libexec/LeaseApp_pmdbServer apps/leaseApp/pmdbServer/pmdbServer.go
	go build ${ASAN_LDFLAGS} -o libexec/leaseClient apps/leaseApp/client/leaseClient.go


install:
	cp libexec/NKV_pmdbServer ${DIR}/libexec/niova/
	cp libexec/NKV_proxy ${DIR}/libexec/niova/
	cp libexec/nkvc ${DIR}/libexec/niova/
	cp libexec/covid_app_server ${DIR}/libexec/niova/
	cp libexec/covid_app_client ${DIR}/libexec/niova/
	cp libexec/vaccinations.csv ${DIR}/libexec/niova/
	cp libexec/foodpalaceappserver ${DIR}/libexec/niova/
	cp libexec/foodpalaceappclient ${DIR}/libexec/niova/
	cp libexec/LeaseApp_pmdbServer ${DIR}/libexec/niova/
	cp libexec/leaseClient ${DIR}/libexec/niova/

clean:
	rm -rf libexec

