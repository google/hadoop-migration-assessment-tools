# How to Contribute

We'd love to accept your patches and contributions to this project.

## Before you begin

### Sign our Contributor License Agreement

Contributions to this project must be accompanied by a
[Contributor License Agreement](https://cla.developers.google.com/about) (CLA).
You (or your employer) retain the copyright to your contribution; this simply
gives us permission to use and redistribute your contributions as part of the
project.

If you or your current employer have already signed the Google CLA (even if it
was for a different project), you probably don't need to do it again.

Visit <https://cla.developers.google.com/> to see your current agreements or to
sign a new one.

### Review our Community Guidelines

This project follows [Google's Open Source Community
Guidelines](https://opensource.google/conduct/).

## Contribution process

### Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Contributing to Logging Hook

### Dependencies

Install bazel:

```
sudo apt-get update
sudo apt-get install bazel
```

### Checkout and compile

To check out and compile the logging hook use the following commands:

```
git clone https://github.com/google/hadoop-migration-assessment-tools.git
cd hadoop-migration-assessment-tools/
bazel build //dist:all
```

Zip file will be available under `bazel-bin/dist` directory.

### Testing

Run all tests:

```
bazel test //...
```
