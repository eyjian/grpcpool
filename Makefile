# Writed by yijian on 2020/12/02
SUBDIRS=test

.PHONY: build
build:
	@for subdir in $(SUBDIRS); do \
		make -C $$subdir; \
	done
