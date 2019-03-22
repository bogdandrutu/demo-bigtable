# Demo Java Apps

## Build and Run
```bash

$ git clone https://github.com/bogdandrutu/java-demo-apps.git
$ cd java-demo-apps
$ ./gradlew installDist
$ export DEMO_APP_MAIN_OPTS="-Dbigtable.projectID=PROJECT -Dbigtable.instanceID=INSTANCE" && \
./build/install/java-demo-apps/bin/demo-app-main

```

## Format code
```bash
./gradlew goJF
```