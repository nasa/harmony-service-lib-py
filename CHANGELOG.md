## 2020/10/20

* Bug fix: Handle Authentication HTTP headers & cookies correctly so
  downloads from TEA work whether or not TEA is in the same AWS region
  as the app making the request.

## [v0.0.24] - 2020/10/13

* Add optional --harmony-sources command line parameter that specifies an external
  JSON file containing a "sources" key to be added to the --harmony-input message.
  As our supported granule count grows, we need to externalize the list of file URLs
  or we run into limits on command length.  We are currently keeping the remainder of
  the message directly on the CLI to allow easier manipulation in workflow definitions.

## [v0.0.23] - 2020/09/22

* Add POST functionality to the `harmony.util.download` function when query
  parameters are included in the function call.

## [v0.0.22] - 2020/09/15

* Fixes regression: when the Adapter raises an exception, the cli will re-raise
  the exception so that it retains the details of the original error.

## [v0.0.21] - 2020/09/09

* Adds ability to accept an Earthdata Login access token and make download requests
  using that token, falling back to the application's EDL username / password
  credentials if the token isn't provided.

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
