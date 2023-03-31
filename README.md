# [github.com/strongo/dalgo2gaedatastore](https://github.com/strongo/dalgo2gaedatastore)

Bridge of Google Cloud Datastore API for [github.com/strongo/dalgo](https://github.com/strongo/dalgo) interface.

This uses [`cloud.google.com/go/datastore`](https://pkg.go.dev/cloud.google.com/go/datastore) package.

## Why "cloud.google.com/go/datastore" and not "google.golang.org/appengine/v2/datastore"?

The difference between the two Go packages "cloud.google.com/go/datastore" and "
google.golang.org/appengine/v2/datastore" lies in the Google Cloud products they are designed to work with and their
functionality.

### `cloud.google.com/go/datastore`:

This package is the official Go client library for Google Cloud Datastore, which is a highly-scalable, NoSQL database
for web and mobile applications provided by Google Cloud Platform. It can be used with various Google Cloud products and
services, not just App Engine. The package enables your application to interact with the Datastore API, allowing you to
store, query, and manage data.

The "cloud.google.com/go/datastore" package is recommended for new projects that require Datastore functionality. It
supports all the features of the Datastore API, and works with both App Engine standard and flexible environments, as
well as other environments like Kubernetes Engine or Compute Engine.

### `google.golang.org/appengine/v2/datastore:

This package is part of the App Engine SDK for Go and is designed specifically for use with the App Engine Standard
Environment. It provides a slightly different API for working with Datastore compared to the
"cloud.google.com/go/datastore" package. The package is tailored to the App Engine environment, which means it may not
be suitable for use outside of App Engine or in newer runtime environments like the second generation of the App Engine
standard environment.

### In summary, the main differences between the two packages are their target environments and scope.

The "cloud.google.com/go/datastore" package is recommended for new projects
as it is more versatile and works across various Google Cloud products and services,
whereas the "google.golang.org/appengine/v2/datastore" package is primarily meant
for use with the App Engine standard environment.

## Similar projects

* [github.com/strongo/dalgo2firestore](https://github.com/strongo/dalgo2firestore)