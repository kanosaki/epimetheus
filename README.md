# Epimetheus: A distributed / memory-centric time series database

// This project is under development.

Epimetheus is a scalable time series database.

Epimetheus goals are:

* Prometheus compatible API
* Highly scalable
* Better query response time
* Easy cluster management

We use Apache Ignite memory-centric data grid as a foundation.
For example, Prometheus API will be deployed as Ignite Service and data will be stored in a cache.

So you can configure Epimetheus basic behavior through Apache Ignite configuration,
such as storage capacity/location(in-memory or disk), consistency, replication and cluster management.


