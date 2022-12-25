# Changelog

## 1.0.1 - December 24, 2022

### 🔧 Fixed

* Require `sdss-drift>=1.0.1` to prevent an issue with reconnecting to a modbus source.


## 1.0.0 - December 24, 2022

(These are only the highlights. I haven't been very good at keeping a change log or versioning for Cerebro).

### 🚀 New

* Added support for LCO, including `LCOWeather` source that queries the LCO weather database.
* Added `ActorClientSource` source that connects to an actor directly, without using Tron.
* Added TPM load script to `bin/`.

### 🏷️ Changed

* Removed the `run_id` tag.

### 🔧 Fixed

* Prevent `DriftSource` hanging by using `sdss-drift>=0.4.5`.


## 0.2.0 - December 15, 2021

### 🚀 New

* Profiles for APO and LVM lab testing. General improvements.


## 0.1.1 - May 19, 2021

### 🚀 New

* [#10](https://github.com/sdss/cerebro/issues/10) When `Cerebro` starts it now runs a Unix server on `/tmp/cerebro.sock` that can be used to communicate with the instance. Currently there are two CLI commands, `cerebro status` and `cerebro restart <source>` that can be used to list the status of the running sources and to restart them.

### ✨ Improved

* Better handling of errors when a source starts.

### 🔧 Fixed

* Package description is now correctly set.


## 0.1.0 - May 19, 2021

### 🚀 New

* Initial version.
