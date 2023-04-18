# Hadoop Migration Assessment Tools

This repository contains tools to enable Hadoop users in collecting required data for Migration Assessment service.

## License

Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Building 

Install bazel:

```
sudo apt-get update
sudo apt-get install bazel
```

Build the dist zip files:

```
bazel build //dist:all
```

Zip files will be available under `bazel-bin/dist` directory.

## Testing

Run all tests:

```
bazel test //...
```

