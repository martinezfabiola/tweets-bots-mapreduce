projectname:=findbottweets
ProjectName:=FindBotTweets
PkgName    :=org.gkfmms.java.bottweetsfinder

HADOOP_CLASSPATH := $(shell hadoop classpath)

jar: build/$(projectname).jar

build/${projectname}.jar: build/org/$(projectname)/$(ProjectName).class
	jar -cvf $@ -C build/ .

build/org/${projectname}/${ProjectName}.class: src/$(ProjectName).java
	mkdir -p build
	javac -cp ${HADOOP_CLASSPATH} src/*.java -d build -Xlint

run: jar
	hadoop fs -rm -f -r mapreduce
	hadoop fs -mkdir -p mapreduce
	hadoop jar build/$(projectname).jar $(PkgName).$(ProjectName) input mapreduce

clean:
	rm -rf build

start:
	start-dfs.sh

clean-data:
	hadoop fs -rm -f -r input
	hadoop fs -mkdir -p input

data: clean-data
	hadoop fs -put data/dataset.txt input

data-small: clean-data
	hadoop fs -put data/dataset-small.txt input

showOutput:
	hadoop fs -cat mapreduce/output/*

showAllOutput:
	hadoop fs -cat mapreduce/*/*

.PHONY: clean start data data-small clean-data

