#!/bin/bash

DB_CONFIG=~/tcache2/db.yaml REDIS_CONFIG=~/tcache2/redis.yaml MAP_TOURS_DB_WORKER_CONFIG=~/tcache2/map_tours_db_worker.yaml PARTNERS_TOURS_DB_WORKER_CONFIG=~/tcache2/partners_tours_db_worker.yaml ./data2db_worker
