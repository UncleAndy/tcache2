all: sletat_loader manager worker data2db_manager data2db_worker post_map_tours_price_logs post_partners_tours_clean
sletat_loader: apps/loaders/sletat_loader.go apps/loaders/sletat/* cache/* db/* apps/workers/worker_base/* tours/*
	go build apps/loaders/sletat_loader.go
manager: apps/workers/manager.go apps/workers/* cache/* db/* tours/*
	go build apps/workers/manager.go
worker: apps/workers/worker.go apps/workers/* cache/* db/* tours/*
	go build apps/workers/worker.go
data2db_manager: apps/data2db/data2db_manager.go cache/* db/* apps/data2db/* tours/*
	go build apps/data2db/data2db_manager.go
data2db_worker: apps/data2db/data2db_worker.go cache/* db/* apps/data2db/* tours/*
	go build apps/data2db/data2db_worker.go
post_map_tours_price_logs: apps/postprocessor/* cache/* tours/*
	go build apps/postprocessor/post_map_tours_price_logs.go
post_partners_tours_clean: apps/postprocessor/* cache/* tours/*
	go build apps/postprocessor/post_partners_tours_clean.go
test:
	cd ./tests && go test -v
