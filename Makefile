all: sletat_loader manager worker data2db_manager data2db_worker post_map_tours_price_logs post_partners_tours_clean
sletat_loader: apps/loaders/sletat_loader.go
	go build apps/loaders/sletat_loader.go
manager: apps/workers/manager.go apps/workers/*
	go build apps/workers/manager.go
worker: apps/workers/worker.go
	go build apps/workers/worker.go
data2db_manager: apps/data2db/data2db_manager.go
	go build apps/data2db/data2db_manager.go
data2db_worker: apps/data2db/data2db_worker.go
	go build apps/data2db/data2db_worker.go
post_map_tours_price_logs: apps/postprocessor/post_map_tours_price_logs.go
	go build apps/postprocessor/post_map_tours_price_logs.go
post_partners_tours_clean: apps/postprocessor/post_partners_tours_clean.go
	go build apps/postprocessor/post_partners_tours_clean.go
test:
	cd ./tests && go test -v
