# Simple log cruncher

A CLI tool which reads a file where each line (separated by `0xA` byte) is an arbitrary JSON object that includes
a field called `type`. It outputs a table containing the number of objects with each `type`, and their total size in
bytes.

----

## Usage

_log-cruncher -p &lt;file path&gt;_