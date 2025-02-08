# librados-rs

Async librados bindings for Rust

## Features

- Cluster
    - [x] List pools
    - [x] Create pool
    - [x] Delete pool
    - [ ] Mom command
    - [ ] Mgr command
    - [ ] Osd command
    - [ ] Pg command
- Pool
    - [x] Get object
    - [x] Show usage
    - [x] List objects
    - [ ] Copy All Contents
    - [ ] Pipeline
    - [x] Snapshot
        - [x] List snaps
        - [x] Create snap
        - [x] Remove snap
        - [x] Rollback
- Object
    - [x] Create
    - [x] Read
    - [x] Write
    - [x] Append
    - [x] Delete
    - [x] Stat
    - [x] Truncate
    - [x] GetXattr
    - [x] SetXattr
    - [x] RemoveXattr
    - [x] ListXattrs
    - [ ] Watch
    - [ ] Notify
    - [ ] List watchers
    - [ ] Lock

## TODO

- [ ] Multiple namespaces
 
