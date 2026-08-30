build:
	docker-compose up -d --build
run:
	docker-compose down && docker-compose up -d

down-volumes:
	docker-compose down --volumes

stop:
	docker-compose down

test:
	make -C gopher-kvs test

cover:
	make -C gopher-kvs cover