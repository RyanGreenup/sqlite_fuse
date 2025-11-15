# SQLite FUSE

## Overview

This creates a FUSE file system for notes that is connected to a sqlite database

## Usage

1. git clone
2. `just run`

## Notes










This is a work in progress

- TODO folders table needs user_id
  - TODO user_id needs to be a varialbe in either ./src/database.rs or ./src/fuse_fs.rs


## Notes

When a user creates some directories like so:

```sh
mkdir -p  ~/Downloads/eg_fuse/a/b/c && mkdir -p ~/Downloads/eg_fuse/1/2/3

```

then moves one under the other:

```sh
➜ mv ~/Downloads/eg_fuse/a ~/Downloads/eg_fuse/1/

```

The STDOUT log reports the following:

```

[DEBUG] lookup: parent=1, name=a
[DEBUG] lookup: parent=1, name=1
[DEBUG] lookup: parent=1, name=1
[DEBUG] lookup: parent=5, name=a
[DEBUG] lookup: Path /1/a not found in database
[DEBUG] lookup: parent=5, name=a
[DEBUG] lookup: Path /1/a not found in database
[DEBUG] lookup: parent=5, name=a
[DEBUG] lookup: Path /1/a not found in database
[DEBUG] rename: a -> a

```


It appears to work perfectly fine, however does the following line indicate a potential error, or is it normal and expected / desirable?


```

[DEBUG] lookup: Path /1/a not found in database

```





This is likely normal because we must check it doesn't exist before performing the move.

This shows:

1. Exists in source:

```

[DEBUG] lookup: parent=1, name=a
[DEBUG] lookup: parent=1, name=1
[DEBUG] lookup: parent=1, name=1
[DEBUG] lookup: parent=5, name=a

```


2. Doesn't exist in target

```

[DEBUG] lookup: Path /1/a not found in database
[DEBUG] lookup: parent=5, name=a
[DEBUG] lookup: Path /1/a not found in database
[DEBUG] lookup: parent=5, name=a
[DEBUG] lookup: Path /1/a not found in database

```



3. Move occurs

```

[DEBUG] rename: a -> a

```



