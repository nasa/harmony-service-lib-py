## [v0.0.10] - 2020/06/18

* Updates the install instructions in the README that use the Earthdata Maven repository
* Update the README with up-to-date release instructions

## [v0.0.9]

### Changed

* Adds a new environment variable LOCALSTACK_HOST that is preferred over BACKEND_HOST
  when developing & testing with LocalStack.

## [v0.0.8]

### Changed

* Replace '/' with '_' in filenames when a variable contains slashes such as /science/grids/data/amplitude
* Handles canceled jobs

## [v0.0.7] 

### Changed

* Add a CHANGELOG.
* Update the release instructions in README.
* Add Bamboo links to the README.
* When the environment variable `USE_LOCALSTACK='true'`, the library will now
  try to use the `BACKEND_HOST` environment variable when connecting to S3,
  defaulting to `localhost` if it is not set.
