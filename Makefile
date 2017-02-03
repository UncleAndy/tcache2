all: apps/loaders/sletat_loader.go apps/workers/manager.go apps/workers/worker.go apps/data2db/data2db_manager apps/data2db/data2db_worker apps/postprocessor/post_map_tours_price_logs
	go build apps/loaders/sletat_loader.go
	go build apps/workers/manager.go
	go build apps/workers/worker.go
	go build apps/data2db/data2db_manager.go
	go build apps/data2db/data2db_worker.go
	go build apps/postprocessor/post_map_tours_price_logs.go
test:
	cd ./tests && go test -v
