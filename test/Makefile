# Writed by yijian on 2020/12/02

.PHONY: build
build:
	@which protoc > /dev/null 2>&1; if test $$? -ne 0; then echo -e "No command '\033[1;33mprotoc\033[m' found, please check environment variable '\033[1;33mPATH\033[m'."; exit 1; fi; \
	protoc --proto_path=. --go_out=plugins=grpc,paths=source_relative:. hello.proto; \
	go build -o grpc_client grpc_client.go hello.pb.go # 编译出依赖 libc 程序; \
	go build -o grpc_server -ldflags '-linkmode "external" -extldflags "-static"' grpc_server.go hello.pb.go # 编译出不依赖 libc 程序

.PHONY: clean
clean:
	rm -f grpc_server grpc_client
