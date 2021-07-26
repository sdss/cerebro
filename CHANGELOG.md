# Changelog

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
