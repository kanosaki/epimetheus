package epimetheus.storage

import org.apache.ignite.Ignite

/**
 * Read intensive, capacity efficient storage area.
 * Affinity is defined by fingerprint of metrics to store datum evenly.
 */
class Aged(val ignite: Ignite) {

}