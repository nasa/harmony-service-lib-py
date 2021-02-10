## 2021/2/10

* A full reworking of the Earthdata Login (EDL) shared / federated
  authentication. This is an implementation change only and does not
  affect the public API or behavior other than to enable certain
  downloads to succeed where they had failed on previous
  releases. Specifically the package should now fully support all
  endpoints hosting granule / data files that will be downloaded for
  Harmony services.

Upgrading:

* This should *not* require any code changes. Update dependency
  specification (if necessary) to use the new release and, e.g.,
  rebuild a Docker image for the service which uses it.

## 2020/11/12

* Adds full support for Earthdata Login (EDL) Bearer tokens. The
  Service Lib now acquires a shared EDL token based on the user's
  access token and Harmony EDL credentials. This shared token is used
  to download assets from backend data sources that support EDL
  federated tokens, including TEA backends.
* Adds a feature flag FALLBACK_AUTHN_ENABLED that can be used to
  download assets from data sources that do not support EDL bearer
  tokens. This defaults to False. This flag should be enabled with
  caution, since it will distort download metrics and can result in
  users downloading data for which they have not approved a EULA.
* Fixes an issue when running tests that required setting certain
  environment variables. Now the configuration has default values that
  allow tests to run without setting them. Note that downloads will
  fail in a production environment if environment vars are not
  properly set.
* Adds validation to the environment variables at startup and will
  fail to startup if required variables are unset. It will also warn
  of other conditions, and will output the value of all environment
  variables at a debug level of 'INFO'.

Upgrading:

* When upgrading, be sure to set all required environment
  variables. See the README for an explanation of all variables, their
  meaning and use, and under what conditions they are required or
  optional.


## 2020/11/09

* Deprecates callback-style invocations in favor of STAC invocations.  Everything is
  backward compatible, but service authors will need to update in order to support
  chaining.

## [v0.0.25] - 2020/10/20

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
